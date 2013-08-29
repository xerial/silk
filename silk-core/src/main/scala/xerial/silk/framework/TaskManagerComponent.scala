//--------------------------------------
//
// TaskManagerComponent.scala
// Since: 2013/06/13 16:57
//
//--------------------------------------

package xerial.silk.framework

import java.util.UUID
import java.util.concurrent.Executors
import xerial.silk.util.{Guard, ThreadUtil}
import xerial.core.log.{LogLevel, Logger}
import java.lang.reflect.InvocationTargetException
import xerial.silk.core.ClosureSerializer
import xerial.core.util.Timer
import scala.language.existentials


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

case class TaskStatusUpdate(taskID: UUID, newStatus: TaskStatus)


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
      debug(s"submit task: ${task}")
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

    lazy val threadManager = Executors.newCachedThreadPool()

    /**
     * Execute a given task in this local executor
     * @param task
     */
    def execute(classBoxID: UUID, task: TaskRequest): Unit = {

      threadManager.submit(new Runnable {
        def run() {
          val t = time(s"task ${task.id.prefix}", LogLevel.TRACE) {

            info(s"Execute task: $task")
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
          info(f"Finished $task. elapsed: ${t.toHumanReadableFormat(t.elapsedSeconds)}")
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


/**
 * TaskManager resides on a master node, and dispatches tasks to client nodes
 */
trait TaskManagerComponent extends Tasks with LifeCycle {
  self: TaskMonitorComponent with ResourceManagerComponent =>

  val taskManager: TaskManager

  trait TaskManager extends Guard with Logger {

    val t = Executors.newCachedThreadPool(new ThreadUtil.DaemonThreadFactory)
    private val allocatedResource = collection.mutable.Map[UUID, NodeResource]()

    /**
     * Receive a task request, acquire a resource for running it
     * then dispatch to a remote node.
     * @param request
     */
    def receive(request: TaskRequest) = {
      debug(s"Received a task request: $request")
      taskMonitor.setStatus(request.id, TaskReceived)
      val preferredNode = request.locality.headOption
      val r = ResourceRequest(preferredNode, 1, None) // Request CPU = 1

      // Create a new thread for waiting resource acquisition. Then dispatches a task to
      // the allocated node.
      // Actor will receive task completion (abort) messages.
      t.submit {
        new Runnable {
          def run() {
            // Resource acquisition is a blocking operation
            val acquired = resourceManager.acquireResource(r)
            allocatedResource += request.id -> acquired
            for (nodeRef <- resourceManager.getNodeRef(acquired.nodeName)) {
              dispatchTask(nodeRef, request)
            }
          }
        }
      }
    }

    def receive(update: TaskStatusUpdate): Unit = guard {
      def release {
        // Release an allocated resource
        allocatedResource.get(update.taskID).map {
          resource =>
            resourceManager.releaseResource(resource)
            allocatedResource -= update.taskID
        }
      }
      update.newStatus match {
        case TaskFinished(nodeName) =>
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






