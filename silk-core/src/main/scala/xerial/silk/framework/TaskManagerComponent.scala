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
import xerial.silk.core.{SilkSerializer, ClosureSerializer, LazyF0}
import xerial.silk.{MissingOp, SilkException, Partitioner, Silk}
import xerial.core.io.IOUtil
import java.net.URL
import xerial.core.util.{Timer, DataUnit}
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



trait TaskRequest extends IDUtil {

  def id: UUID

  /**
   * ID of the ClassBox in which execute this task
   * @return
   */
  def classBoxID: UUID

  /**
   * Preferred locations (node names) to execute this task
   * @return
   */
  def locality: Seq[String]


  def description : String
  def execute(localClient:LocalClient)
}

object TaskRequest extends Logger {

  import SilkSerializer._

  private[silk] def deserializeClosure(ser:Array[Byte]) = {
    trace(s"deserializing the closure")
    val closure = deserializeObj[AnyRef](ser)
    trace(s"Deserialized the closure: ${closure.getClass}")
    closure
  }

}


trait IDUtil {

  implicit class IDPrefix(id: UUID) {
    def prefix2 = id.toString.substring(0, 2)
    def prefix = id.toString.substring(0, 8)
    def path = s"$prefix2/$prefix"
  }

}

import xerial.silk.core.SilkSerializer._


trait Tasks extends IDUtil {

  type Task <: TaskRequest

  implicit class RichTaskStatus(status: TaskStatus) {
    def serialize = serializeObj(status)
  }
  implicit class RichTask(task: Task) {
    def serialize = serializeObj(task)
  }

  implicit class TaskDeserializer(b: Array[Byte]) {
    def asTaskStatus: TaskStatus = deserializeObj[TaskStatus](b)
    def asTask: Task = deserializeObj[Task](b)
  }
}

import TaskRequest._

/**
 * Message object for actor
 * @param id
 * @param serializedClosure
 * @param locality
 */
case class TaskRequestF0(id: UUID, classBoxID: UUID, serializedClosure: Array[Byte], locality: Seq[String]) extends TaskRequest {
  override def toString = s"TaskRequestF0(${id.prefix}, locality:${locality.mkString(", ")})"
  def description = "F0"

  def execute(localClient:LocalClient) {
    // Find apply[A](v:A) method.
    // Function0 class of Scala contains apply(v:Object) method, so we avoid it by checking the presence of parameter types.
    val closure = deserializeClosure(serializedClosure)
    val cl = closure.getClass
    cl.getMethods.filter(mt => mt.getName == "apply" && mt.getParameterTypes.length == 0).headOption match {
      case Some(applyMt) =>
        // Run the task in this node
        applyMt.invoke(closure)
      case _ =>
        throw MissingOp(s"missing apply method in $cl")
    }
  }

}


case class TaskRequestF1(description:String, id: UUID, classBoxID: UUID, serializedClosure: Array[Byte], locality: Seq[String]) extends TaskRequest {
  override def toString = s"[${id.prefix}] $description, locality:${locality.mkString(", ")})"

  def execute(localClient:LocalClient) {
    val closure = deserializeClosure(serializedClosure)
    val cl = closure.getClass
    cl.getMethods.filter(mt => mt.getName == "apply" && mt.getParameterTypes.length == 1).headOption match {
      case Some(applyMt) =>
        applyMt.invoke(closure, localClient)
      case _ =>
        throw MissingOp(s"missing apply(x) method in $cl")
    }
  }
}

case class DownloadTask(id:UUID, classBoxID:UUID, resultID:UUID, dataAddress:URL, splitID:Int, locality:Seq[String]) extends TaskRequest with Logger {
  override def toString = s"DownloadTask(${id.prefix}, ${resultID.prefix}, ${dataAddress})"

  def description = "Download task"

  def execute(localClient:LocalClient) {
    try {
      val slice = Slice(localClient.currentNodeName, -1, splitID)
      IOUtil.readFully(dataAddress.openStream) {
        data => info(s"Received the data $dataAddress, size:${DataUnit.toHumanReadableFormat(data.size)}")
          localClient.sliceStorage.putRaw(resultID, splitID, slice, data)
      }
    }
    catch {
      case e:Exception =>
        localClient.sliceStorage.poke(resultID, splitID)
        throw e
    }
  }
}


case class EvalSliceTask(description:String, id: UUID, classBoxID:UUID, opid:UUID, inid: UUID, inputSlice: Slice, f: Seq[_] => Any, locality:Seq[String]) extends TaskRequest {

  def execute(localClient:LocalClient) {
    try {
      val si = inputSlice.index
      // TODO: Error handling when slice is not found in the storage
      val data = localClient.sliceStorage.retrieve(inid, inputSlice)
      val slice = Slice(localClient.currentNodeName, -1, si)

      val result = f(data) match {
        case seq: Seq[_] => seq
        case silk: Silk[_] =>
        // recursively evaluate (for flatMap)
          val nestedResult = for (future <- localClient.executor.getSlices(silk)) yield {
            val nestedSlice = future.get
            localClient.sliceStorage.retrieve(silk.id, nestedSlice)
          }
          nestedResult.seq.flatten
      }
      localClient.sliceStorage.put(opid, si, slice, result)
      // TODO If all slices are evaluated, mark StageFinished
    }
    catch {
      case e: Throwable =>
        localClient.sliceStorage.poke(opid, inputSlice.index)
        throw e
    }
  }
}

case class ReduceTask(description:String, id:UUID, classBoxID:UUID, opid: UUID, inid: UUID, inputSliceIndexes: Seq[Int], outputSliceIndex: Int, reducer: Seq[_] => Any, aggregator: Seq[_] => Any, locality:Seq[String]) extends TaskRequest with Logger {

  def execute(localClient:LocalClient) {
    try {
      debug(s"eval reduce: input slice indexes(${inputSliceIndexes.mkString(", ")}), output slice index:$outputSliceIndex")
      val reduced = for (si <- inputSliceIndexes.par) yield {
        val slice = localClient.sliceStorage.get(inid, si).get
        val data = localClient.sliceStorage.retrieve(inid, slice)
        reducer(data)
      }
      val aggregated = aggregator(reduced.seq)

      val sl = Slice(localClient.currentNodeName, -1, outputSliceIndex)
      localClient.sliceStorage.put(opid, outputSliceIndex, sl, IndexedSeq(aggregated))
      // TODO If all slices are evaluated, mark StageFinished
    }
    catch {
      case e: Throwable =>
        localClient.sliceStorage.poke(opid, outputSliceIndex)
        throw e
    }
  }
}

case class ShuffleTask(description:String, id:UUID, classBoxID:UUID, opid: UUID, inid: UUID, inputSlice: Slice, partitioner: Partitioner[_], locality:Seq[String]) extends TaskRequest {
  def execute(localClient:LocalClient) {
      try {
        val si = inputSlice.index
        // TODO: Error handling when slice is not found in the storage
        val data = localClient.sliceStorage.retrieve(inid, inputSlice)
        val pp = partitioner.asInstanceOf[Partitioner[Any]]
        for ((p, lst) <- data.groupBy(pp.partition(_))) {
          val slice = Slice(localClient.currentNodeName, p, si)
          localClient.sliceStorage.putSlice(opid, p, si, slice, lst)
        }
        // TODO If all slices are evaluated, mark StageFinished
      }
      catch {
        case e: Throwable =>
          // TODO notify all waiters
          localClient.sliceStorage.poke(opid, inputSlice.index)
          throw e
      }

  }
}

case class ShuffleReduceTask(description:String, id:UUID, classBoxID:UUID, opid: UUID, inid: UUID, keyIndex: Int, numInputSlices: Int, ord: Ordering[_], locality:Seq[String]) extends TaskRequest {

  def execute(localClient:LocalClient) {
    try {
      val input = for (i <- (0 until numInputSlices).par) yield {
        val inputSlice = localClient.sliceStorage.getSlice(inid, keyIndex, i).get
        val data = localClient.sliceStorage.retrieve(inid, inputSlice)
        data
      }
      val result = input.flatten.seq.sorted(ord.asInstanceOf[Ordering[Any]])
      localClient.sliceStorage.put(opid, keyIndex, Slice(localClient.currentNodeName, -1, keyIndex), result)
    }
    catch {
      case e: Throwable =>
        localClient.sliceStorage.poke(opid, 0)
        throw e
    }
  }
}



/**
 * LocalTaskManager is deployed at each host and manages task execution.
 * Each task is processed like a transaction, which records started/finished/failed(aborted) logs.
 *
 */
trait LocalTaskManagerComponent extends Tasks {
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






