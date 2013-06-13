//--------------------------------------
//
// SilkFramework.scala
// Since: 2013/06/09 11:44
//
//--------------------------------------

package xerial.silk.framework

import scala.language.higherKinds
import scala.language.experimental.macros
import scala.reflect.ClassTag
import xerial.silk.mini._
import xerial.silk.{SilkException, SilkError}
import xerial.core.log.Logger
import java.util.UUID
import java.net.InetAddress


/**
 * SilkFramework is an abstraction of input and result data types of Silk operations.
 *
 * @author Taro L. Saito
 */
trait SilkFramework extends LoggingComponent {

  /**
   * Silk is an abstraction of data processing operation. By calling run method, its result can be obtained
   * @tparam A
   */
  type Silk[A] = SilkMini[A]
  type Result[A] = Seq[A]
  type Future[A] = SilkFuture[A]

  /**
   * Future reference to a result
   * @tparam A
   */
  type ResultRef[A] = Future[Result[A]]




    /**
   * Helper functions
   */
  protected object helper {
    def fwrap[A,B](f:A=>B) = f.asInstanceOf[Any=>Any]
    def filterWrap[A](f:A=>Boolean) = f.asInstanceOf[Any=>Boolean]
    def rwrap[P, Q, R](f: (P, Q) => R) = f.asInstanceOf[(Any, Any) => Any]

  }
}

trait SilkRunner extends SilkFramework {

  /**
   * Run the given Silk operation and return the result
   * @param silk
   * @return
   */
  def run[A](silk: Silk[A]): Result[A]


  /**
   * Run a specific target (val or function name) used in a given silk operation.
   * @param silk
   * @param targetName
   * @tparam A
   * @return
   */
  def run[A](silk:Silk[A], targetName:String) : Result[_]

}


trait LifeCycle {

  def startUp {}
  def tearDown {}
}


trait ProgramTreeComponent {
  self:SilkFramework =>

  def graphOf[A](op:Silk[A]) = SilkMini.createCallGraph(op)

  /**
   * Find a part of the silk tree
   * @param silk
   * @param targetName
   * @tparam A
   * @return
   */
  def collectTarget[A](silk:Silk[A], targetName:String) : Seq[Silk[_]] = {
    info(s"Find target $targetName from $silk")
    val g = graphOf(silk)
    debug(s"call graph: $g")

    g.nodes.collect{
      case op if op.fref.localValName.map(_ == targetName) getOrElse false =>
        op
    }
  }

  def findTarget[A](silk:Silk[A], targetName:String) : Option[Silk[_]] = {
    val matchingOps = collectTarget(silk, targetName)
    matchingOps.size match {
      case v if v > 1 => throw new IllegalArgumentException(s"more than one target is found for $targetName")
      case other => matchingOps.headOption
    }
  }

  def descentandsOf[A](silk:Silk[A]) : Set[Silk[_]] = {
    val g = graphOf(silk)
    g.descendantsOf(silk)
  }

    def descentandsOf[A](silk:Silk[A], targetName:String) : Set[Silk[_]] = {
    val g = graphOf(silk)
    findTarget(silk, targetName) match {
      case Some(x) => g.descendantsOf(x)
      case None => Set.empty
    }
  }

}



/**
 * A standard implementation of partial evaluation
 */
trait PartialEvaluator extends SilkRunner with ProgramTreeComponent {

  def run[A](silk:Silk[A], targetName:String) : Result[_] = {
    val matchingOps = findTarget(silk, targetName)
    matchingOps match {
      case None => throw new IllegalArgumentException(s"target $targetName is not found")
      case Some(x) => run(x)
    }
  }

}


/**
 * Session manages already computed data.
 */
trait SessionComponent  {
  self: SilkFramework =>

  type Session <: SessionAPI
  val session: Session

  trait SessionAPI {
    def sessionID : UUID
    def sessionIDPrefix = sessionID.toString.substring(0, 8)

    /**
     * We would like to provide newSilk but it needs a macro-based implementation,
     * which cannot be used to implement (override) the interface, so the
     * implementation of this API needs explicit coding of this method.
     */
    // def newSilk[A](in:Seq[A]) : Silk[A]

    /**
     * Get the result of an silk
     * @param op
     * @tparam A
     * @return
     */
    def get[A](op:Silk[A]) : ResultRef[A]

    /**
     * Get teh result of the target in the given silk
     * @param op
     * @param target
     * @tparam A
     * @return
     */
    def get[A](op:Silk[A], target:String) : ResultRef[_]

    /**
     * Set or replace the result of the target silk
     * @param op
     * @param result
     * @tparam A
     */
    def set[A](op:Silk[A], result:ResultRef[A])

    /**
     * Clear the the result of the target silk and its dependent results
     * @param op
     * @tparam A
     */
    def clear[A](op:Silk[A])

  }

  def run[A](session:Session, op:Silk[A]) : ResultRef[A]

}





/**
 * A standard implementation of Session
 */
trait StandardSessionImpl
  extends SessionComponent
  with ProgramTreeComponent {
  self:SilkFramework with CacheComponent =>

//  type Session = SessionImpl
//
//  class SessionImpl(val sessionID:UUID) extends SessionAPI {
//
//    def get[A](op: Silk[A]) = {
//      val r = cache.getOrElseUpdate(op, run(this, op))
//      r.asInstanceOf[ResultRef[A]]
//    }
//
//    def get[A](op: Silk[A], target: String) = {
//      val subOps = findTarget(op, target)
//      subOps match {
//        case None => throw new IllegalArgumentException(s"$target is not found")
//        case Some(x) => get(x)
//      }
//    }
//
//    def set[A](op: Silk[A], result: ResultRef[A]) : Unit = {
//      cache.update(op, result)
//    }
//
//    def clear[A](op: Silk[A]) : Unit = {
//      cache.remove(op)
//      for(d <- descentandsOf(op))
//        cache.remove(d)
//    }
//
//    def clear : Unit = {
//      cache.clear
//    }
//  }
//

}



trait SliceComponent {

  self:SilkFramework =>

  /**
   * Slice might be a future
   * @tparam A
   */
  type Slice[A] <: SliceAPI[A]

  trait SliceAPI[A] {
    def index: Int
    def data: Seq[A]
  }


}


/**
 * Executor of the Silk program
 */
trait ExecutorComponent
  extends SilkRunner
  with SliceComponent
  with StageManagerComponent
  with SliceStorageComponent {

  type Executor <: ExecutorAPI
  def executor : Executor

  def newSlice[A](op:Silk[_], index:Int, data:Seq[A]) : Slice[A]

  def run[A](silk: Silk[A]): Result[A] = {
    val result = executor.getSlices(silk).flatMap(_.data)
    result
  }

  trait ExecutorAPI {
    def defaultParallelism : Int = 2

    def evalRecursively[A](op:SilkMini[A], v:Any) : Seq[Slice[A]] = {
      v match {
        case silk:Silk[_] => getSlices(silk).asInstanceOf[Seq[Slice[A]]]
        case seq:Seq[_] => Seq(newSlice(op, 0, seq.asInstanceOf[Seq[A]]))
        case e => Seq(newSlice(op, 0, Seq(e).asInstanceOf[Seq[A]]))
      }
    }

    private def flattenSlices[A](op:SilkMini[_], in: Seq[Seq[Slice[A]]]): Seq[Slice[A]] = {
      var counter = 0
      val result = for (ss <- in; s <- ss) yield {
        val r = newSlice(op, counter, s.data)
        counter += 1
        r
      }
      result
    }

    def processSlice[A,U](slice:Slice[A])(f:Slice[A]=>Slice[U]) : Slice[U] = {
      f(slice)
    }

    def getSlices[A](op: Silk[A]) : Seq[Slice[A]] = {
      import helper._
      try {
        stageManager.startStage(op)
        val result : Seq[Slice[A]] = op match {
          case m @ MapOp(fref, in, f, fe) =>
            val slices = for(slc <- getSlices(in)) yield {
              newSlice(op, slc.index, slc.data.map(m.fwrap).asInstanceOf[Seq[A]])
            }
            slices
          case m @ FlatMapOp(fref, in, f, fe) =>
            val nestedSlices = for(slc <- getSlices(in)) yield {
              slc.data.flatMap(e => evalRecursively(op, m.fwrap(e)))
            }
            flattenSlices(op, nestedSlices)
          case FilterOp(fref, in, f, fe) =>
            val slices = for(slc <- getSlices(in)) yield
              newSlice(op, slc.index, slc.data.filter(filterWrap(f)))
            slices
          case ReduceOp(fref, in, f, fe) =>
            val rf = rwrap(f)
            val reduced : Seq[Any] = for(slc <- getSlices(in)) yield
              slc.data.reduce(rf)
            val resultSlice = newSlice(op, 0, Seq(reduced.reduce(rf))).asInstanceOf[Slice[A]]
            Seq(resultSlice)
          case RawSeq(fc, in) =>
            val w = (in.length + (defaultParallelism - 1)) / defaultParallelism
            val split = for((split, i) <- in.sliding(w, w).zipWithIndex) yield
              newSlice(op, i, split)
            split.toIndexedSeq
          case other =>
            warn(s"unknown op: $other")
            Seq.empty
        }
        stageManager.finishStage(op)
        result
      }
      catch {
        case e:Exception =>
          stageManager.abortStage(op)
          throw SilkException.pending
      }
    }
  }
}


trait DefaultExecutor
  extends ExecutorComponent {

  type Executor = ExecutorImpl
  def executor = new ExecutorImpl

  class ExecutorImpl extends ExecutorAPI
}


/**
 * @author Taro L. Saito
 */
trait SliceStorageComponent extends SliceComponent {
  self: SilkFramework =>

  val sliceStorage: SliceStorageAPI

  trait SliceStorageAPI {
    def get(op: Silk[_], index: Int): Future[Slice[_]]
    def put(op: Silk[_], index: Int, slice: Slice[_]): Unit
    def contains(op: Silk[_], index: Int): Boolean
  }
}


/**
 * Managing running state of
 */
trait StageManagerComponent extends SilkFramework {

  type StageManager <: StageManagerAPI
  val stageManager: StageManagerAPI

  trait StageManagerAPI {
    /**
     * Call this method when an evaluation of the given Silk expression has started
     * @param op
     * @return Future of the all slices
     */
    def startStage[A](op:Silk[A])

    def finishStage[A](op:Silk[A])

    def abortStage[A](op:Silk[A])

    /**
     * Returns true if the evaluation of the Silk expression has finished
     * @param op
     * @return
     */
    def isFinished[A](op: Silk[A]): Boolean
  }

}




trait DistributedFramework
  extends SilkFramework {


}

trait MasterComponent
  extends DistributedFramework
  with TaskSchedulerComponent {

}

trait ClientComponent
  extends DistributedFramework {


}

/**
 * Representing worker node
 * @param name
 * @param address
 * @param clientPort
 * @param dataServerPort
 */
case class Node(name:String,
                address:String,
                pid:Int,
                clientPort:Int,
                dataServerPort:Int,
                resource:NodeResource) {
  def host = Host(name, address)
  def toRef = NodeRef(name, address, clientPort)
}

case class NodeRef(name:String, address:String, clientPort:Int) {
  def host = Host(name, address)
}

object Host {
  def apply(s:String) : Host = {
    val lh = InetAddress.getByName(s)
    Host(s, lh.getHostAddress)
  }
}

case class Host(name: String, address: String) {
  def prefix = name.split("\\.")(0)
}



case class NodeResource(nodeName:String, numCPUs:Int, memorySize:Long) {

  private def ensureSameNode(n:String) {
    require(nodeName == n, "must be the same node")
  }

  private def ensureNonNegative(v:Long) = if(v < 0) 0L else v

  def adjustFor(r:ResourceRequest) = {
    NodeResource(nodeName, r.cpu, r.memorySize.getOrElse(-1))
  }

  def -(r:NodeResource) = {
    ensureSameNode(r.nodeName)
    NodeResource(nodeName, numCPUs - r.numCPUs, memorySize - ensureNonNegative(r.memorySize))
  }

  def +(r:NodeResource) = {
    ensureSameNode(r.nodeName)
    NodeResource(nodeName, numCPUs + r.numCPUs, memorySize + ensureNonNegative(r.memorySize))
  }

  def isEnoughFor(r:ResourceRequest) : Boolean = {
    r.cpu <= numCPUs && r.memorySize.map(_ <= memorySize).getOrElse(true)
  }
}

case class ResourceRequest(nodeName:Option[String], cpu:Int, memorySize:Option[Long])


trait ClusterManagerComponent {

  type NodeManager <: NodeManagerAPI
  val nodeManager : NodeManager

  def clientIsActive(nodeName:String) : Boolean

  trait NodeManagerAPI {
    def nodes : Seq[Node]
    def addNode(n:Node)
    def removeNode(nodeName:String)
  }
}

/**
 * ResourceManager runs on a master node
 */
trait ResourceManagerComponent {

  type ResourceManager <: ResourceManagerAPI

  val resourceManager : ResourceManager

  trait ResourceManagerAPI {
    /**
     * Acquire the resource. This operation blocks until
     */
    def acquireResource(r:ResourceRequest) : NodeResource
    def releaseResource(r:NodeResource)
    def lostResourceOf(nodeName:String)
  }

}

trait Tasks {

  type Task <: TaskAPI
  /**
   * Interface for computing a result at remote machine
   */
  trait TaskAPI {

    def id: Int

    /**
     * Run and return the results. The returned value is mainly used for accumulated results, which is small enough to serialize
     * @return
     */
    def run: Any

    /**
     * Preferred location to execute this task
     * @return
     */
    def locality: Seq[String]
  }

  trait TaskEventListener {
    def onCompletion(task:Task, result:Any)
    def onFailure(task:Task)
  }

}



trait TaskSchedulerComponent extends Tasks {
  self: DistributedFramework =>

  type TaskManager <: TaskManagerAPI
  val taskManager : TaskManager


  trait TaskManagerAPI {
    def submit(task:Task) = {

    }
  }
}


trait Task





