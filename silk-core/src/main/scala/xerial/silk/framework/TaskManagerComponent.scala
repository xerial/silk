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


trait TaskAPI {

  def id: UUID
  def idPrefix2 = id.toString.substring(0, 2)
  def idPrefix = id.toString.substring(0, 8)

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


case class LocalTask(id:UUID, taskBinary:Array[Byte], locality:Seq[String]) extends TaskAPI


trait Tasks {

  type Task <: TaskAPI

  /**
   * Interface for computing a result at remote machine
   */
  trait TaskEventListener {
    def onCompletion(task:Task, result:Any)
    def onFailure(task:Task)
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

}



/**
 * LocalTaskManager is deployed at each host and manages task execution.
 * Each task is managed like a transaction, which records started/finished/failed(aborted) logs.
 *
 */
trait LocalTaskManager extends Tasks {
  self: TaskMonitor =>

  val localTaskManager : LocalTaskManager
  val currentNodeName : String

  trait LocalTaskManager {
    /**
     * Submit a task to local task manager, then the task will be sent to the master
     * @param task
     */
    def submit(task:Task)

    def status(taskID:UUID) : TaskStatus = {
      taskMonitor.getStatus(taskID)
    }

    def stop(taskID:UUID) {
      // TODO track local running tasks

    }

    def execute(task:Task) = {
      taskMonitor.setStatus(task, TaskStarted(currentNodeName))
      val exec = SilkMini.deserializeObj(task.taskBinary)
      // TODO exec function
    }

  }

}

trait TaskMonitor extends Tasks {

  val taskMonitor : TaskMonitor

  trait TaskMonitor {
    def setStatus(task:Task, status:TaskStatus)
    def getStatus(taskID:UUID) : TaskStatus

    def waitCompletion(taskID:UUID) : SilkFuture[TaskStatus]
  }

}

trait TaskManagerComponent extends Tasks with LifeCycle {
  self: TaskMonitor with ResourceManagerComponent =>

  val taskManager : TaskManager

  trait TaskManager {

    val t = Executors.newCachedThreadPool(new ThreadUtil.DaemonThreadFactory)

    /**
     * Receive a task, acquire a resource for running a task,
     * then dispatch a task to a remote node.
     * @param task
     */
    def receive(task:Task) = {
      taskMonitor.setStatus(task, TaskReceived)
      val r = if(task.locality.isEmpty)
        ResourceRequest(None, 1, None) // CPU = 1
      else
        ResourceRequest(task.locality.headOption, 1, None)


      t.submit {
        new Runnable {
          def run() {
            // Resource acquisition is a blocking operation
            val acquired = resourceManager.acquireResource(r)
            submitTask(acquired.nodeName, task)
            taskMonitor.waitCompletion(task.id).map { status =>
              // Release acquired resource
              resourceManager.releaseResource(acquired)
            }
          }
        }
      }
    }

    def submitTask(nodeName:String, task:Task)

    def close { t.shutdown() }

  }

  abstract override def startUp {
    super.startUp
  }

  abstract override def tearDown {
    taskManager.close
    super.tearDown
  }

}






