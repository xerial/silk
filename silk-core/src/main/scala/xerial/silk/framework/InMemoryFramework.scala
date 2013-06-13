package xerial.silk.framework

import xerial.core.log.Logger
import xerial.silk.mini._
import scala.language.experimental.macros
import scala.language.higherKinds

import scala.reflect.ClassTag
import java.util.UUID

/**
 * A base trait for in-memory implementation of the SilkFramework
 */
trait InMemoryFramework
  extends SilkFramework
  with PartialEvaluator
  with DefaultConsoleLogger {

  /**
   * Create a new instance of Silk from a given input sequence
   * @param in
   * @param ev
   * @tparam A
   * @return
   */
  def newSilk[A](in: Result[A])(implicit ev: ClassTag[A]): Silk[A] = macro SilkMini.newSilkImpl[A]

}


/**
 * The simplest SilkFramework implementation that process all the data in memory.
 * @author Taro L. Saito
 */
trait InMemoryRunner extends InMemoryFramework {

  /**
   * Evaluate the silk operation and return the result
   * @param silk
   * @tparam A
   * @return
   */
  def run[A](silk: Silk[A]): Result[A] = {
    /**
     * Cast the result type to
     * @param v
     */
    implicit class Cast(v: Any) {
      def cast: Result[A] = v.asInstanceOf[Result[A]]
    }

    def eval(v: Any): Any = {
      v match {
        case s: Silk[_] => run(s)
        case other => other
      }
    }

    def evalSeq(seq: Any): Seq[Any] = {
      seq match {
        case s: Silk[_] => run(s)
        case other => other.asInstanceOf[Seq[Any]]
      }
    }

    import xerial.silk.mini._
    import helper._
    silk match {
      case RawSeq(fref, in) => in.cast
      case MapOp(fref, in, f, fe) =>
        run(in).map(e => eval(fwrap(f)(e))).cast
      case FlatMapOp(fref, in, f, fe) =>
        run(in).flatMap(e => evalSeq(fwrap(f)(e))).cast
      case FilterOp(fref, in, f, fe) =>
        run(in).filter(f).cast
      case ReduceOp(fref, in, f, fe) =>
        Seq(run(in).reduce(f)).cast
      case other =>
        warn(s"unknown silk type: $silk")
        Seq.empty
    }

  }
}


trait InMemorySliceStorage extends SliceStorageComponent {
  self: SilkFramework =>

  val sliceStorage = new SliceStorageAPI with Guard {
    private val table = collection.mutable.Map[(UUID, Int), Slice[_]]()
    private val futureToResolve = collection.mutable.Map[(UUID, Int), Future[Slice[_]]]()

    def get(op: Silk[_], index: Int): Future[Slice[_]] = guard {
      val key = (op.uuid, index)
      if (futureToResolve.contains(key)) {
        futureToResolve(key)
      }
      else {
        val f = new SilkFutureMultiThread[Slice[_]]
        if (table.contains(key)) {
          f.set(table(key))
        }
        else
          futureToResolve += key -> f
        f
      }
    }

    def put(op: Silk[_], index: Int, slice: Slice[_]) {
      guard {
        val key = (op.uuid, index)
        if (!table.contains(key)) {
          table += key -> slice
        }
        if (futureToResolve.contains(key)) {
          futureToResolve(key).set(slice)
          futureToResolve -= key
        }
      }
    }

    def contains(op: Silk[_], index: Int) = guard {
      val key = (op.uuid, index)
      table.contains(key)
    }
  }


}


trait InMemorySliceExecutor
  extends InMemoryFramework
  with DefaultExecutor
  with InMemorySliceStorage
  with InMemoryStageManager {

  // Uses a locally stored slice for evaluation
  type Slice[V] = LocalSlice[V]

  case class LocalSlice[A](index: Int, data: Result[A]) extends SliceAPI[A]

  def newSlice[A](op: Silk[_], index: Int, data: Seq[A]): Slice[A] = {
    LocalSlice(index, data)
  }
}


trait InMemoryStageManager extends StageManagerComponent {
  type StageManger = StageManagerImpl
  val stageManager = new StageManagerImpl

  class StageManagerImpl extends StageManagerAPI {
    def abortStage[A](op: Silk[A]) {}
    def isFinished[A](op: Silk[A]): Boolean = false
    def startStage[A](op: Silk[A]) {
      trace(s"[${op.idPrefix}] started stage")
    }
    def finishStage[A](op: Silk[A]) {
      trace(s"[${op.idPrefix}] finished stage")
    }
  }

}