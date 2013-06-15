//--------------------------------------
//
// TaskManagerComponent.scala
// Since: 2013/06/13 16:57
//
//--------------------------------------

package xerial.silk.framework

import xerial.silk.mini.SilkMini
import java.util.UUID
import java.util.concurrent.Executors
import xerial.silk.util.ThreadUtil
import xerial.core.log.Logger
import java.lang.reflect.InvocationTargetException
import xerial.silk.core.LazyF0


trait TaskAPI {

  def id: UUID

  /**
   * The serialized function to execute.
   * @return
   */
  def taskBinary: Array[Byte]

  /**
   * Preferred locations (node names) to execute this task
   * @return
   */
  def locality: Seq[String]
}


case class TaskRequest(id:UUID, taskBinary:Array[Byte], locality:Seq[String]) extends TaskAPI




trait Tasks {

  type Task <: TaskAPI

  implicit class IDPrefix(id:UUID) {
    def prefix2 = id.toString.substring(0, 2)
    def prefix = id.toString.substring(0, 8)
  }

  implicit class RichTaskStatus(status:TaskStatus) {
    def serialize = SilkMini.serializeObj(status)
  }
  implicit class RichTask(task:Task) {
    def serialize = SilkMini.serializeObj(task)
  }

  implicit class TaskDeserializer(b:Array[Byte]) {
    def asTaskStatus : TaskStatus = SilkMini.deserializeObj[TaskStatus](b)
    def asTask : Task = SilkMini.deserializeObj[Task](b)
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
 * Transaction record of task execution
 */
sealed trait TaskStatus
case object TaskMissing extends TaskStatus
case object TaskReceived extends TaskStatus
case class TaskStarted(nodeName:String) extends TaskStatus
case class TaskFinished(nodeName:String) extends TaskStatus
case class TaskFailed(message: String) extends TaskStatus


/**
 * LocalTaskManager is deployed at each host and manages task execution.
 * Each task is managed like a transaction, which records started/finished/failed(aborted) logs.
 *
 */
trait LocalTaskManagerComponent extends Tasks {
  self: TaskMonitorComponent =>

    val localTaskManager : LocalTaskManager
  def currentNodeName : String

  trait LocalTaskManager extends Logger {

    def submit[R](f: => R) : TaskRequest = {
      val l = LazyF0(f)
      val task = TaskRequest(UUID.randomUUID(), SilkMini.serializeObj(l.functionInstance), Seq.empty)
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

    def sendToMaster(task:TaskRequest)


    def status(taskID:UUID) : TaskStatus = {
      taskMonitor.getStatus(taskID)
    }

    def stop(taskID:UUID) {
      // TODO track local running tasks
      warn("not yet implemented")
    }

    def execute(task:TaskRequest) = {
      taskMonitor.setStatus(task.id, TaskStarted(currentNodeName))
      val closure = SilkMini.deserializeObj[Any](task.taskBinary) // closure
      val cl = closure.getClass
      trace(s"Deserialized the closure: ${cl}")
      for(applyMt <-
          cl.getMethods.filter(mt => mt.getName == "apply" && mt.getParameterTypes.length == 0).headOption) {
        try {
          applyMt.invoke(closure)
          taskMonitor.setStatus(task.id, TaskFinished(currentNodeName))
        }
        catch {
          case e: InvocationTargetException =>
            error(e.getTargetException)
            taskMonitor.setStatus(task.id, TaskFailed(e.getTargetException.getMessage))
          case e : Throwable =>
            error(e)
            taskMonitor.setStatus(task.id, TaskFailed(e.getMessage))
        }
      }
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

case class SubmitTask(taskID:UUID, serializedClosure:Array[Byte], locality:Seq[String])
case class RunTask(taskID:UUID, serializedClosure:Array[Byte])


/**
 * TaskManager resides on a master node, and dispatches tasks to client nodes
 */
trait TaskManagerComponent extends Tasks with LifeCycle {
  self: TaskMonitorComponent with ResourceManagerComponent =>

  val taskManager : TaskManager

  trait TaskManager extends Logger {

    val t = Executors.newCachedThreadPool(new ThreadUtil.DaemonThreadFactory)

    /**
     * Receive a task request, acquire a resource for running it
     * then dispatch to a remote node.
     * @param request
     */
    def receive(request:TaskRequest) = {
      taskMonitor.setStatus(request.id, TaskReceived)
      val preferredNode = request.locality.headOption
      val r = ResourceRequest(preferredNode, 1, None) // Request CPU = 1
      t.submit {
        new Runnable {
          def run() {
            // Resource acquisition is a blocking operation
            val acquired = resourceManager.acquireResource(r)
            for(nodeRef <- resourceManager.getNodeRef(acquired.nodeName)) {
              dispatchTask(nodeRef, request)
              val future = taskMonitor.completionFuture(request.id)
              future.respond { status =>
                // Release acquired resource
                resourceManager.releaseResource(acquired)
              }
            }
          }
        }
      }
    }

    /**
     * Dispatch a task to a remote node
     * @param node
     * @param task
     */
    def dispatchTask(node:NodeRef, task:TaskRequest)

    def close { t.shutdown() }

  }

  abstract override def startup {
    super.startup
  }

  abstract override def teardown {
    taskManager.close
    super.teardown
  }

}






