//--------------------------------------
//
// TaskManagerComponent.scala
// Since: 2013/06/13 16:57
//
//--------------------------------------

package xerial.silk.framework

import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, Executors}
import xerial.silk.util.{Guard, ThreadUtil}
import xerial.core.log.{LogLevel, Logger}
import java.lang.reflect.InvocationTargetException
import xerial.silk.core.ClosureSerializer
import xerial.core.util.Timer
import scala.language.existentials
import xerial.silk.TimeOut



/**
 * Transaction record of task execution
 */
sealed trait TaskStatus
case object TaskMissing extends TaskStatus
case object TaskReceived extends TaskStatus
case class TaskStarted(nodeName: String) extends TaskStatus

/**
 * Task finished record
 * @param nodeName
 */
case class TaskFinished(nodeName: String) extends TaskStatus
case class TaskFailed(nodeName: String, message: String) extends TaskStatus

case class TaskStatusUpdate(taskID: UUID, newStatus: TaskStatus) extends IDUtil {
  override def toString = s"[${taskID.prefix}] $newStatus"
}


trait IDUtil {

  implicit class IDPrefix(id: UUID) {
    def prefix2 = id.toString.substring(0, 2)
    def prefix = id.toString.substring(0, 8)
    def path = s"$prefix2/$prefix"
  }

}
/**
 * LocalTaskManager is deployed at each host and manages task execution.
 * Each task is processed like a transaction, which records started/finished/failed(aborted) logs.
 *
 */
trait LocalTaskManagerComponent extends Tasks with IDUtil {
  self: SilkFramework
    with TaskMonitorComponent
    with LocalClientComponent
    with ClassBoxComponent =>

  val localTaskManager: LocalTaskManager

  trait LocalTaskManager extends Timer with Logger {

    def submit[R](cbid: UUID, locality: Seq[String] = Seq.empty)(f: => R): TaskRequest = {
      // TODO Get class box ID somewhere
      val task = TaskRequestF0(UUID.randomUUID(), cbid, ClosureSerializer.serializeClosure(f), locality)
      submit(task)
      task
    }

    /**
     * Send a task from this local task manager to the master
     * @param task
     */
    def submit(task: TaskRequest) = {
      debug(s"Submit task: ${task.summary}")
      sendToMaster(task)
      task
    }

    /**
     * Send the task to the master node
     * @param task
     */
    protected def sendToMaster(task: TaskRequest)
    protected def sendToMaster(taskID: UUID, status: TaskStatus)

    def updateTaskStatus(taskID: UUID, status: TaskStatus) {
      taskMonitor.setStatus(taskID, status)
      sendToMaster(taskID, status)
    }

    /**
     * Get the status of a given task
     * @param taskID
     * @return
     */
    def status(taskID: UUID): TaskStatus = {
      taskMonitor.getStatus(taskID)
    }

    /**
     * Stop the task
     * @param taskID
     */
    def stop(taskID: UUID) {
      // TODO track local running tasks
      warn("not yet implemented")
    }


    def withClassLoader[U](classBoxID: UUID)(f: => U) = {
      val cl = getClassBox(classBoxID).classLoader
      val prevCl = Thread.currentThread.getContextClassLoader
      try {
        Thread.currentThread.setContextClassLoader(cl)
        f
      }
      finally {
        Thread.currentThread.setContextClassLoader(prevCl)
      }
    }

    lazy val threadManager = Executors.newFixedThreadPool(Runtime.getRuntime.availableProcessors * 2)

    /**
     * Execute a given task in this local executor
     * @param task
     */
    def execute(classBoxID: UUID, task: TaskRequest): Unit = {

      threadManager.submit(new Runnable {
        def run() {
          val t = time(s"task ${task.id.prefix}", LogLevel.TRACE) {

            info(s"Execute ${task.summary}")
            val nodeName = localClient.currentNodeName

            // Record TaskStarted (transaction start)
            updateTaskStatus(task.id, TaskStarted(nodeName))

            val taskStatus = try {
              withClassLoader(classBoxID) {
                // Execute the task
                task.execute(localClient)
                // Task Commit
                TaskFinished(nodeName)
              }
            }
            catch {
              // Abort
              case e: InvocationTargetException =>
                error(e.getTargetException)
                TaskFailed(nodeName, e.getTargetException.getMessage)
              case e: Throwable =>
                error(e)
                TaskFailed(nodeName, e.getMessage)
            }

            // Tell the task status to the subsequent tasks
            updateTaskStatus(task.id, taskStatus)
          }
          info(f"Finished ${task.summary}. elapsed: ${t.toHumanReadableFormat(t.elapsedSeconds)}")
        }
      })
    }
  }
}

/**
 * Compoenent for monitoring task status
 */
trait TaskMonitorComponent extends Tasks {

  val taskMonitor: TaskMonitor

  trait TaskMonitor {
    /**
     * Set the task status
     * @param taskID
     * @param status
     */
    def setStatus(taskID: UUID, status: TaskStatus)
    def getStatus(taskID: UUID): TaskStatus

    def completionFuture(taskID: UUID): SilkFuture[TaskStatus]
  }

}

import scala.collection.JavaConversions._
/**
 * TaskManager resides on a master node, and dispatches tasks to client nodes
 */
trait TaskManagerComponent extends Tasks with LifeCycle {
  self: TaskMonitorComponent with ResourceManagerComponent =>

  val taskManager: TaskManager

  trait TaskManager extends Guard with Logger {

    val t = Executors.newCachedThreadPool(new ThreadUtil.DaemonThreadFactory)

    val allocatedResource = new ConcurrentHashMap[UUID, NodeResource]()

    /**
     * Receive a task request, acquire a resource for running it
     * then dispatch to a remote node.
     * @param request
     */
    def receive(request: TaskRequest) = {
      debug(s"Received a task request: ${request.summary}")
      taskMonitor.setStatus(request.id, TaskReceived)
      val preferredNode = request.locality.headOption
      val r = ResourceRequest(preferredNode, 1, None) // Request CPU = 1
      val id = request.id.prefix
      // Create a new thread for waiting resource acquisition. Then dispatches a task to
      // the allocated node.
      // Actor will receive task completion (abort) messages.
      t.submit {
        new Runnable {
          def run() {
            try {
              // Resource acquisition is a blocking operation
              debug(s"Ask a resource for task [$id]")
              val acquired = resourceManager.acquireResource(r)
              allocatedResource += request.id -> acquired
              debug(s"Acquired a resource for task [$id]: $acquired")
              for(nodeRef <- resourceManager.getNodeRef(acquired.nodeName)) {
                debug(s"Dispatch task [${request.id.prefix}] to ${nodeRef.name}")
                dispatchTask(nodeRef, request)
              }
            }
            catch {
              case e:TimeOut =>
                warn(s"Timeout: failed to acquire resource for task [${id}]")
                taskMonitor.setStatus(request.id, TaskFailed("master", "Timeout: failed to acquire resource"))
            }
          }
        }
      }
    }

    def receive(update: TaskStatusUpdate): Unit = guard {
      info(update)
      def release {
        // Release an allocated resource
        val id = update.taskID
        if(allocatedResource.containsKey(id)) {
          val resource = allocatedResource.get(id)
          resourceManager.releaseResource(resource)
          allocatedResource -= id
        }
        else {
          warn(s"No allocated resource for task [${update.taskID.prefix}] is found")
        }
      }
      update.newStatus match {
        case tf@TaskFinished(nodeName) =>
          release
        case TaskFailed(nodeName, message) =>
          release
        case other =>
      }
    }

    /**
     * Dispatch a task to a remote node
     * @param node
     * @param task
     */
    def dispatchTask(node: NodeRef, task: TaskRequest)

    /**
     * Close the task manager
     */
    def close {
      t.shutdown()
    }

  }

  abstract override def startup {
    super.startup
    // TODO Launch a thread that monitors task completions and aborts

  }

  abstract override def teardown {
    taskManager.close
    super.teardown
  }

}






