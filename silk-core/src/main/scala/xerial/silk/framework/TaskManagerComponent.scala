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
import xerial.core.log.Logger
import java.lang.reflect.InvocationTargetException
import xerial.silk.core.{ClosureSerializer, LazyF0}


trait TaskAPI {

  def id: UUID

  /**
   * The serialized function to execute.
   * @return
   */
  def serializedClosure: Array[Byte]

  /**
   * Preferred locations (node names) to execute this task
   * @return
   */
  def locality: Seq[String]
}


trait IDUtil {

  implicit class IDPrefix(id:UUID) {
    def prefix2 = id.toString.substring(0, 2)
    def prefix = id.toString.substring(0, 8)
    def path = s"$prefix2/$prefix"
  }

}

import xerial.silk.core.SilkSerializer._


trait Tasks extends IDUtil {

  type Task <: TaskAPI

  implicit class RichTaskStatus(status:TaskStatus) {
    def serialize = serializeObj(status)
  }
  implicit class RichTask(task:Task) {
    def serialize = serializeObj(task)
  }

  implicit class TaskDeserializer(b:Array[Byte]) {
    def asTaskStatus : TaskStatus = deserializeObj[TaskStatus](b)
    def asTask : Task = deserializeObj[Task](b)
  }


  /**
   * Interface for computing a result at remote machine
   */
  trait TaskEventListener {
    def onCompletion(task:Task, result:Any)
    def onFailure(task:Task)
  }

}

/**
 * Message object for actor
 * @param id
 * @param serializedClosure
 * @param locality
 */
case class TaskRequest(id:UUID, serializedClosure:Array[Byte], locality:Seq[String]) extends TaskAPI

/**
 * Transaction record of task execution
 */
sealed trait TaskStatus
case object TaskMissing extends TaskStatus
case object TaskReceived extends TaskStatus
case class TaskStarted(nodeName:String) extends TaskStatus

/**
 * Task finished record
 * @param nodeName
 */
case class TaskFinished(nodeName:String) extends TaskStatus
case class TaskFailed(nodeName:String, message: String) extends TaskStatus

case class TaskStatusUpdate(taskID:UUID, newStatus:TaskStatus)

/**
 * LocalTaskManager is deployed at each host and manages task execution.
 * Each task is processed like a transaction, which records started/finished/failed(aborted) logs.
 *
 */
trait LocalTaskManagerComponent extends Tasks {
  self: TaskMonitorComponent =>

  val localTaskManager : LocalTaskManager
  def currentNodeName : String

  trait LocalTaskManager extends Logger {

    def submit[R](f: => R) : TaskRequest = {
      val task = TaskRequest(UUID.randomUUID(), ClosureSerializer.serializeClosure(f), Seq.empty)
      submit(task)
      task
    }



    /**
     * Send a task from this local task manager to the master
     * @param task
     */
    def submit(task:TaskRequest) {
      sendToMaster(task)
    }

    /**
     * Send the task to the master node
     * @param task
     */
    protected def sendToMaster(task:TaskRequest)
    protected def sendToMaster(taskID:UUID, status:TaskStatus)

    def updateTaskStatus(taskID:UUID, status:TaskStatus) {
      taskMonitor.setStatus(taskID, status)
      sendToMaster(taskID, status)
    }

    /**
     * Get the status of a given task
     * @param taskID
     * @return
     */
    def status(taskID:UUID) : TaskStatus = {
      taskMonitor.getStatus(taskID)
    }

    /**
     * Stop the task
     * @param taskID
     */
    def stop(taskID:UUID) {
      // TODO track local running tasks
      warn("not yet implemented")
    }

    def execute(task:TaskRequest) : Unit = {
      // Record TaskStarted (transaction start)
      updateTaskStatus(task.id, TaskStarted(currentNodeName))

      // Deserialize the closure
      val closure = deserializeObj[Any](task.serializedClosure)
      val cl = closure.getClass
      trace(s"Deserialized the closure: ${cl}")

      // Find apply[A](v:A) method.
      // Function0 class of Scala contains apply(v:Object) method, so we avoid it by checking the presence of parameter types.
      for(applyMt <-
          cl.getMethods.filter(mt => mt.getName == "apply" && mt.getParameterTypes.length == 0).headOption) {
        try {
          // Run the task in this node
          applyMt.invoke(closure)
          // Record TaskFinished (commit)
          updateTaskStatus(task.id, TaskFinished(currentNodeName))
        }
        catch {
          // Abort
          case e: InvocationTargetException =>
            error(e.getTargetException)
            updateTaskStatus(task.id, TaskFailed(currentNodeName, e.getTargetException.getMessage))
          case e : Throwable =>
            error(e)
            updateTaskStatus(task.id, TaskFailed(currentNodeName, e.getMessage))
        }
      }
      // TODO: Error handling when apply() method is not found
    }
  }

}

/**
 * Compoenent for monitoring task status
 */
trait TaskMonitorComponent extends Tasks {

  val taskMonitor : TaskMonitor

  trait TaskMonitor {
    /**
     * Set the task status
     * @param taskID
     * @param status
     */
    def setStatus(taskID:UUID, status:TaskStatus)
    def getStatus(taskID:UUID) : TaskStatus

    def completionFuture(taskID:UUID) : SilkFuture[TaskStatus]
  }

}




/**
 * TaskManager resides on a master node, and dispatches tasks to client nodes
 */
trait TaskManagerComponent extends Tasks with LifeCycle {
  self: TaskMonitorComponent with ResourceManagerComponent =>

  val taskManager : TaskManager

  trait TaskManager extends Guard with Logger {

    val t = Executors.newCachedThreadPool(new ThreadUtil.DaemonThreadFactory)
    private val allocatedResource = collection.mutable.Map[UUID, NodeResource]()

    /**
     * Receive a task request, acquire a resource for running it
     * then dispatch to a remote node.
     * @param request
     */
    def receive(request:TaskRequest) = {
      taskMonitor.setStatus(request.id, TaskReceived)
      val preferredNode = request.locality.headOption
      val r = ResourceRequest(preferredNode, 1, None) // Request CPU = 1

      // Launch a new thread for waiting task completion.
      // TODO: It would be better to avoid creating a thread for each task. Let the actor receive task completion (abort) messages.
      t.submit {
        new Runnable {
          def run() {
            // Resource acquisition is a blocking operation
            val acquired = resourceManager.acquireResource(r)
            allocatedResource += request.id -> acquired
            for(nodeRef <- resourceManager.getNodeRef(acquired.nodeName)) {
              dispatchTask(nodeRef, request)
            }

//            // Await the task completion
//            val future = taskMonitor.completionFuture(request.id)
//            future.respond { status =>
//            // Release acquired resource
//              resourceManager.releaseResource(acquired)
//            }
          }
        }
      }
    }

    def receive(update:TaskStatusUpdate) : Unit = guard {
      def release {
        // Release an allocated resource
        allocatedResource.get(update.taskID).map { resource =>
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
    def dispatchTask(node:NodeRef, task:TaskRequest)

    /**
     * Close the task manager
     */
    def close { t.shutdown() }

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






