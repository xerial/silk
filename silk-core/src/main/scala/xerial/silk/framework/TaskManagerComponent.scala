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
import xerial.silk.{Partitioner, Silk}


trait TaskRequest {

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

  type Task <: TaskRequest

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
}

/**
 * Message object for actor
 * @param id
 * @param serializedClosure
 * @param locality
 */
case class TaskRequestF0(id:UUID, serializedClosure:Array[Byte], locality:Seq[String]) extends TaskRequest
case class TaskRequestF1(id:UUID, serializedClosure:Array[Byte], locality:Seq[String]) extends TaskRequest


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
  self: SilkFramework
    with TaskMonitorComponent
    with LocalClientComponent =>

  val localTaskManager : LocalTaskManager

  trait LocalTaskManager extends Logger {

    def submit[R](f: => R) : TaskRequest = {
      val task = TaskRequestF0(UUID.randomUUID(), ClosureSerializer.serializeClosure(f), Seq.empty)
      submit(task)
      task
    }

    def submitF1[R](locality:Seq[String]=Seq.empty)(f: LocalClient => R) : TaskRequest = {
      val ser = ClosureSerializer.serializeF1(f)
      val task = TaskRequestF1(UUID.randomUUID(), ser, locality)
      submit(task)
      task
    }

    def submitEvalTask(locality:Seq[String]=Seq.empty)(opid:UUID, inid:UUID, inputSlice:Slice, f:Seq[_]=>Any) : TaskRequest = {
      val ser = ClosureSerializer.serializeF1{ c:LocalClient =>
        c.localTaskManager.evalSlice(opid, inid, inputSlice, f)
      }
      val task = TaskRequestF1(UUID.randomUUID(), ser, locality)
      submit(task)
      task
    }

    def submitReduceTask(opid:UUID, inid:UUID, inputSliceIndexes:Seq[Int], outputSliceIndex:Int, reducer:Seq[_] =>Any, aggregator:Seq[_]=>Any) = {
      val ser = ClosureSerializer.serializeF1{ c:LocalClient =>
        c.localTaskManager.evalReduce(opid, inid, inputSliceIndexes, outputSliceIndex, reducer, aggregator)
      }
      val task = TaskRequestF1(UUID.randomUUID(), ser, Seq.empty)
      submit(task)
      task
    }

    def submitShuffleTask(locality:Seq[String]=Seq.empty)(opid:UUID, inid:UUID, inputSlice:Slice, partitioner:Partitioner[_]) : TaskRequest = {
      val ser = ClosureSerializer.serializeF1{ c:LocalClient =>
        c.localTaskManager.evalPartitioning(opid, inid, inputSlice, partitioner)
      }
      val task = TaskRequestF1(UUID.randomUUID(), ser, locality)
      submit(task)
      task
    }

    def submitShuffleReduceTask(opid:UUID, inid:UUID, keyIndex:Int, numInputSlices:Int) : TaskRequest = {
      val ser = ClosureSerializer.serializeF1{ c:LocalClient =>
        c.localTaskManager.evalShuffleReduce(opid, inid, keyIndex, numInputSlices)
      }
      val task = TaskRequestF1(UUID.randomUUID(), ser, Seq.empty)
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

    /**
     * Execute a given task in this local executor
     * @param task
     */
    def execute(task:TaskRequest) : Unit = {

      val nodeName = localClient.currentNodeName

      // Record TaskStarted (transaction start)
      updateTaskStatus(task.id, TaskStarted(nodeName))

      // Deserialize the closure
      trace(s"deserializing the closure")
      val closure = deserializeObj[AnyRef](task.serializedClosure)
      val cl = closure.getClass
      trace(s"Deserialized the closure: ${cl}")

      try {
        task match {
          case TaskRequestF0(id, _, locality) =>
            // Find apply[A](v:A) method.
            // Function0 class of Scala contains apply(v:Object) method, so we avoid it by checking the presence of parameter types.
            cl.getMethods.filter(mt => mt.getName == "apply" && mt.getParameterTypes.length == 0).headOption match {
              case Some(applyMt) =>
                // Run the task in this node
                applyMt.invoke(closure)
                // Record TaskFinished (commit)
                updateTaskStatus(task.id, TaskFinished(nodeName))
              case _ => updateTaskStatus(task.id, TaskFailed(nodeName, s"missing apply method in $cl"))
            }
          // TODO: Error handling when apply() method is not found
          case TaskRequestF1(id, _, locality) =>
             cl.getMethods.filter(mt => mt.getName == "apply" && mt.getParameterTypes.length == 1).headOption match {
               case Some(applyMt) =>
                 applyMt.invoke(closure, localClient)
                 // Record TaskFinished (commit)
                 updateTaskStatus(task.id, TaskFinished(nodeName))
               case _ => updateTaskStatus(task.id, TaskFailed(nodeName, s"missing apply(x) method in $cl"))
            }
          case _ => updateTaskStatus(task.id, TaskFailed(nodeName, s"unknown request for closure $cl"))
        }
      }
      catch {
        // Abort
        case e: InvocationTargetException =>
          error(e.getTargetException)
          updateTaskStatus(task.id, TaskFailed(nodeName, e.getTargetException.getMessage))
        case e : Throwable =>
          error(e)
          updateTaskStatus(task.id, TaskFailed(nodeName, e.getMessage))
      }
    }

    /**
     * Applying slice evaluation task in this node
     * @param opid
     * @param inid
     * @param inputSlice
     * @param f
     */
    def evalSlice(opid:UUID, inid:UUID, inputSlice:Slice, f:Seq[_]=>Any) {
      try {
        val si = inputSlice.index
        // TODO: Error handling when slice is not found in the storage
        val data = localClient.sliceStorage.retrieve(inid, inputSlice)
        val slice = Slice(localClient.currentNodeName, -1, si)

        val result = f(data) match {
          case seq:Seq[_] => seq
          case silk:Silk[_] =>
            // recursively evaluate (for flatMap)
            val nestedResult = for(future <- localClient.executor.getSlices(silk)) yield {
              val nestedSlice = future.get
              localClient.sliceStorage.retrieve(silk.id, nestedSlice)
            }
            nestedResult.flatten
        }
        localClient.sliceStorage.put(opid, si, slice, result)
        // TODO If all slices are evaluated, mark StageFinished
      }
      catch {
        case e:Throwable =>
          localClient.sliceStorage.poke(opid, inputSlice.index)
          throw e
      }
    }

    def evalReduce(opid:UUID, inid:UUID, inputSliceIndexes:Seq[Int], outputSliceIndex:Int, reducer:Seq[_]=>Any, aggregator:Seq[_]=>Any) {
      try {
        debug(s"eval reduce: ${inputSliceIndexes}, output slice index:$outputSliceIndex")
        val reduced = for(si <- inputSliceIndexes) yield {
          val slice = localClient.sliceStorage.get(inid, si).get
          val data = localClient.sliceStorage.retrieve(inid, slice)
          reducer(data)
        }
        val aggregated = aggregator(reduced)
        val sl = Slice(localClient.currentNodeName, -1, outputSliceIndex)
        localClient.sliceStorage.put(opid, outputSliceIndex, sl, IndexedSeq(aggregated))
        // TODO If all slices are evaluated, mark StageFinished
      }
      catch {
        case e:Throwable =>
          localClient.sliceStorage.poke(opid, outputSliceIndex)
          throw e
      }
    }

    def evalPartitioning(opid:UUID, inid:UUID, inputSlice:Slice, partitioner:Partitioner[_]) {
      try {
        val si = inputSlice.index
        // TODO: Error handling when slice is not found in the storage
        val data = localClient.sliceStorage.retrieve(inid, inputSlice)
        val pp = partitioner.asInstanceOf[Partitioner[Any]]
        for((p, lst) <- data.groupBy(pp.partition(_))) {
          val slice = Slice(localClient.currentNodeName, p, si)
          localClient.sliceStorage.putSlice(opid, p, si, slice, lst)
        }
        // TODO If all slices are evaluated, mark StageFinished
      }
      catch {
        case e:Throwable =>
          // TODO notify all waiters
          localClient.sliceStorage.poke(opid, inputSlice.index)
          throw e
      }
    }

    def evalShuffleReduce(opid:UUID, inid:UUID, keyIndex:Int, numInputSlices:Int) {
      try {
        val input = for(i <- 0 until numInputSlices) yield {
          val inputSlice = localClient.sliceStorage.getSlice(inid, keyIndex, i).get
          val data = localClient.sliceStorage.retrieve(inid, inputSlice)
          data
        }
        val result = input.flatten.toIndexedSeq
        localClient.sliceStorage.put(opid, keyIndex, Slice(localClient.currentNodeName, -1, keyIndex), result)
      }
      catch {
        case e:Throwable =>
          localClient.sliceStorage.poke(opid, 0)
          throw e
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

      // Create a new thread for waiting resource acquisition. Then dispatches a task to
      // the allocated node.
      // Actor will receive task completion (abort) messages.
      t.submit {
        new Runnable {
          def run() {
            // Resource acquisition is a blocking operation
            val acquired = resourceManager.acquireResource(r)
            allocatedResource += request.id -> acquired
            for(nodeRef <- resourceManager.getNodeRef(acquired.nodeName)) {
              dispatchTask(nodeRef, request)
            }
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






