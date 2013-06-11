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
  type Future[A] <: SilkFuture[A]
  /**
   * Future reference to a result
   * @tparam A
   */
  type ResultRef[A] = Future[Result[A]]


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

    /**
   * Helper functions
   */
  protected object helper {
    def fwrap[A,B](f:A=>B) = f.asInstanceOf[Any=>Any]
    def filterWrap[A](f:A=>Boolean) = f.asInstanceOf[Any=>Boolean]
    def rwrap[P, Q, R](f: (P, Q) => R) = f.asInstanceOf[(Any, Any) => Any]

  }
}

trait LifeCycle {

  def start
  def terminate
}


trait ProgramTreeComponent extends SilkFramework {

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
trait PartialEvaluator extends SilkFramework with ProgramTreeComponent {

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
trait SessionComponent extends SilkFramework {

  type Session <: SessionAPI
  val session: Session

  trait SessionAPI {
    def sessionID : UUID
    def sessionIDPrefix = sessionID.toString.substring(0, 8)

    /**
     * Get the result of an silk
     * @param op
     * @tparam A
     * @return
     */
    def get[A](op:Silk[A]) : ResultRef[_]
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
  with ProgramTreeComponent { self:CacheComponent =>

  type Session = SessionImpl

  class SessionImpl(val sessionID:UUID) extends SessionAPI {

    def get[A](op: Silk[A]) = {
      val r = cache.getOrElseUpdate(op, run(this, op))
      r.asInstanceOf[ResultRef[A]]
    }

    def get[A](op: Silk[A], target: String) = {
      val subOps = findTarget(op, target)
      subOps match {
        case None => throw new IllegalArgumentException(s"$target is not found")
        case Some(x) => get(x)
      }
    }

    def set[A](op: Silk[A], result: ResultRef[A]) : Unit = {
      cache.update(op, result)
    }

    def clear[A](op: Silk[A]) : Unit = {
      cache.remove(op)
      for(d <- descentandsOf(op))
        cache.remove(d)
    }

    def clear : Unit = {
      cache.clear
    }
  }


}



trait NodeManagerComponent {

  type Node <: NodeAPI

  type NodeManager <: NodeManagerAPI

  val nodeManager : NodeManager

  trait NodeAPI {
    def name:String
    def address:String
    def port:Int
  }

  trait NodeManagerAPI {
    def nodes : Seq[Node]
    def addNode(n:Node)
    def removeNode(n:Node)
  }

}

trait TaskManagerComponent extends NodeManagerComponent {

  type ResourceManager <: ResourceManagerAPI
  type MachineResource <: MachineResourceAPI

  val resourceManger : ResourceManager

  trait MachineResourceAPI {
    def node: Option[Node]
    def cpu: Int
    def memory: Option[Long]
  }

  trait ResourceManagerAPI {
    def acquireResource(r:MachineResource) : Node
    def acquireResource(node:Node, r:MachineResource) : Node
    def releaseResource(r:MachineResource)
  }

}

trait SliceComponent extends SilkFramework {

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
  extends SilkFramework
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

    def processSlice[A,U](slice:Slice[A])(f:Slice[A]=>U) : U = {
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

  val sliceStorage: SliceStorage

  trait SliceStorage {
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


