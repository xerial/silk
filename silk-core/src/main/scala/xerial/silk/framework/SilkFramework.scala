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
  type Future[A]

  /**
   * Run the given Silk operation and return the result
   * @param silk
   * @return
   */
  def run[A](silk: Silk[A]): Result[A]


  /**
   * Helper functions
   */
  protected object helper {
    def fwrap[A,B](f:A=>B) = f.asInstanceOf[Any=>Any]
    def filterWrap[A](f:A=>Boolean) = f.asInstanceOf[Any=>Boolean]
    def rwrap[P, Q, R](f: (P, Q) => R) = f.asInstanceOf[(Any, Any) => Any]

  }
}



/**
 * Configuration
 */
trait ConfigComponent {
  type Config

  /**
   * Get the current configuration
   * @return
   */
  def config: Config

}




trait EvaluatorComponent extends SilkFramework {

  /**
   * Slice might be a future
   * @tparam A
   */
  type Slice[A] <: SliceAPI[A]
  type Evaluator <: EvaluatorAPI
  def evaluator : Evaluator

  def newSlice[A](op:Silk[_], index:Int, data:Seq[A]) : Slice[A]

  trait SliceAPI[A] {
    def index: Int
    def data: Seq[A]
  }

  def run[A](silk: Silk[A]): Result[A] = {
    val result = evaluator.getSlices(silk).flatMap(_.data)
    result
  }


  trait EvaluatorAPI {
    def defaultParallelism : Int = 2
    def getSlices[A](op:Silk[A]) : Seq[Slice[A]]
    def processSlice[A, U](slice:Slice[A])(f: Slice[A] => U): U
  }
}


trait DefaultEvaluator
  extends EvaluatorComponent
  with StageManagerComponent
  with SliceStorageComponent {


  type Evaluator = EvaluatorImpl
  def evaluator = new EvaluatorImpl

  class EvaluatorImpl extends EvaluatorAPI {

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


/**
 * @author Taro L. Saito
 */
trait SliceStorageComponent extends EvaluatorComponent {

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


