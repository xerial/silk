//--------------------------------------
//
// TaskManager.scala
// Since: 2013/06/07 10:43 AM
//
//--------------------------------------

package xerial.silk.mini


import scala.language.existentials
import xerial.core.log.{LoggerFactory, Logger}

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

  //def addListener(listener: TaskEventListener)
  //def removeListener(listener: TaskEventListener)

  //def addNode(h:Host)
  //def lostNode(h:Host)

}


trait TaskEventListener {
  def onCompletion(task: Task[_], result: Any)

  def onFailure(task: Task[_])
}


abstract class CentralTaskManager extends TaskManager with TaskEventListener with Guard {

  val resourceManager: ResourceManager

  import scala.collection.mutable

  private val submitted = mutable.Queue[Task[_]]()
  private val allocatedResource = mutable.Map[Task[_], WorkerResource]()

  /**
   * Submit a task and send the task to the SilkMaster.
   * @param task
   */
  def submitTask(task: Task[_]) {
    // TODO choose an appropriate amount of CPUs
    val resource = resourceManager.acquireResource(1, -1)
    allocatedResource += task -> resource
    submitted += task

    // Execute
    val b = SilkMini.serializeObj(task)
    runAt(resource.host, b)
  }

  def runAt(host: Host, serializedTask: Array[Byte])

  def onCompletion(task: Task[_], result: Any) {
    guard {
      if (allocatedResource.contains(task)) {
        val resource = allocatedResource(task)
        allocatedResource -= task
        resourceManager.releaseResource(resource)
      }
    }
  }

  def onFailure(task: Task[_]) {

  }

}


trait Master extends Logger {

  val taskManager: TaskManager

  def receive(m: Message) {
    m match {
      case tu@TaskStatusUpdate(executorID, taskID, status) =>
        handleTaskStatusUpdate(tu)
      case other =>
        warn(s"unknown message: $other")
    }
  }

  def handleTaskStatusUpdate(tu: TaskStatusUpdate) {
    tu match {
      case TaskStarted() =>
      case TaskFinished() =>
      case TaskFailed(message) =>
        error(s"[executor-${tu.executorID}] task ${tu.taskID} failed:${message}")

    }
  }

}

trait MasterRef {
  def send(m: Message)
}

trait Message

case class TaskStatusUpdate(executorID: Int, taskID: Int, status: TaskStatus) extends Message


/**
 * SilkEnv holds references to SilkMaster, DistributedCache, etc.
 */
trait SilkEnv extends Logger {

  val master: MasterRef
  val cache: DistributedCache

  def updateStatus(executorID: Int, taskID: Int, status: TaskStatus) {
    info(s"update status: [$taskID] $status")
    // Send the status to the SilkMaster
    master.send(TaskStatusUpdate(executorID, taskID, status))
  }
}


/**
 * Transaction record of task execution
 */
sealed trait TaskStatus

case class TaskStarted() extends TaskStatus

case class TaskFinished() extends TaskStatus

case class TaskFailed(message: String) extends TaskStatus


/**
 * TaskExecutor is deployed at each host and manages task execution.
 * Each task is managed like a transaction, which records started/finished/failed(aborted) logs.
 *
 */
class TaskExecutor(id: Int, host: Host) {

  private val env: SilkEnv = _

  def execute(task: Task[_]) = {

    def execute(task: Task[_]) = {
      try {
        env.updateStatus(id, task.id, TaskStarted())
        task.run
        env.updateStatus(id, task.id, TaskFinished())
      }
      catch {
        case e: Throwable =>
          env.updateStatus(id, task.id, TaskFailed(e.getMessage))
      }
    }

  }
}

// Representing an cluster environment for testing using cake pattern
object TestSilkEnv {

  val cache = new SimpleDistributedCache

  val master = new Master {
    val taskManager = new CentralTaskManager with LocalRunner with TestResourceManager
  }

  val masterRef: MasterRef = new MasterRef {
    def send(m: Message) {
      master.receive(m)
    }
  }

  trait TestResourceManager {
    val resourceManager: ResourceManager = {
      val r = new ResourceManager
      r.addResource(WorkerResource(Host("h1"), 2, 1 * 1024 * 1024))
      r.addResource(WorkerResource(Host("h2"), 2, 1 * 1024 * 1024))
      r
    }
  }

  trait LocalRunner {
    this: TaskManager =>

    private val logger = LoggerFactory(classOf[LocalRunner])
    private val executor = collection.mutable.Map[Host, TaskExecutor]()

    private var executorCount = 0

    def runAt(host: Host, serializedTask: Array[Byte]) = {
      logger.debug(s"runAt $host")

      val ec = executor.getOrElseUpdate(host, {
        executorCount += 1
        val newID = executorCount
        new TaskExecutor(newID, host)
      })

      val task = SilkMini.deserializeObj[Task[_]](serializedTask)
      ec.execute(task)
    }
  }

}

trait TestSilkEnv extends SilkEnv {
  val env = new SilkEnv {
    val master = TestSilkEnv.masterRef
    val cache = TestSilkEnv.cache
  }
}


