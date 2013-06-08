//--------------------------------------
//
// TaskManager.scala
// Since: 2013/06/07 10:43 AM
//
//--------------------------------------

package xerial.silk.mini
import scala.language.existentials
import xerial.core.log.Logger

/**
 * Interface for computing a result at remote machine
 * @tparam Result
 */
trait Task[Result] {

  def id: Int

  /**
   * Run and return the results. The returned value is mainly used for accumulated results, which is small enough to serialize
   * @return
   */
  def run: Result

  /**
   * Preferred location to execute this task
   * @return
   */
  def locality: Seq[Host]
}


/**
 * TaskManager resides in SilkMaster, and dispatches a submitted job to a remote machine.
 *
 *
 * @author Taro L. Saito
 */
trait TaskManager {

  /**
   * Submit a task and send the task to the SilkMaster.
   * @param task
   */
  def submitTask(task: Task[_])
  def addListener(listener: TaskEventListener)
  def removeListener(listener: TaskEventListener)

  //def addNode(h:Host)
  //def lostNode(h:Host)

}


trait TaskEventListener {
  def onCompletion(task: Task[_], result:Any)
  def onFailure(task: Task[_])
}

object SilkEnv {

  private val defaultEnv = new SilkEnv

  def getEnv : SilkEnv = defaultEnv

}


trait Master {


}

class SilkEnv extends Logger {

  //val masterRef : Master = _

  def updateStatus(executorID:Int, taskID:Int, status:TaskStatus) {
    info(s"update status: [$taskID] $status")
    // Send the status to the SilkMaster


  }

}

sealed trait TaskStatus

case class TaskStarted() extends TaskStatus
case class TaskFinished() extends TaskStatus
case class TaskFailed(message:String) extends TaskStatus


/**
 * TaskExecutor is deployed at each host and manages task execution.
 * Each task is managed like a transaction, which records started/finished/failed(aborted) logs.
 *
 */
class TaskExecutor(id:Int, host:Host) {

  private val env = SilkEnv.getEnv

  def execute(task:Task[_]) = {

    try {
      env.updateStatus(id, task.id, TaskStarted())
      task.run
      env.updateStatus(id, task.id, TaskFinished())
    }
    catch {
      case e : Exception =>
        env.updateStatus(id, task.id, TaskFailed(e.getMessage))
    }

  }

}






class CentralTaskManager extends TaskManager {

  private var listeners = Seq.empty[TaskEventListener]
  private val resourceManager = new ResourceManager


  /**
   * Submit a task and send the task to the SilkMaster.
   * @param task
   */
  def submitTask(task: Task[_]) {



  }


  def addListener(listener: TaskEventListener) {
    listeners :+= listener
  }

  def removeListener(listener: TaskEventListener) {
    listeners = listeners.filter(_ eq listener)
  }
}


