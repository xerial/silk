package xerial.silk.framework

import scala.language.experimental.macros
import scala.language.higherKinds

import scala.reflect.ClassTag
import java.util.UUID
import xerial.silk.util.Guard
import xerial.silk.framework.ops.{SilkSeq, SilkMacros}
import xerial.core.log.Logger
import xerial.silk.SilkException

/**
 * A base trait for in-memory implementation of the SilkFramework
 */
trait InMemoryFramework
  extends SilkFramework {


  /**
   * Create a new instance of Silk from a given input sequence
   * @param in
   * @param ev
   * @tparam A
   * @return
   */
  def newSilk[A](in: Seq[A])(implicit ev: ClassTag[A]): SilkSeq[A] = macro SilkMacros.newSilkImpl[A]

}


/**
 * The simplest SilkFramework implementation that process all the data in memory.
 * @author Taro L. Saito
 */
trait InMemoryRunner extends InMemoryFramework with ProgramTreeComponent {

  def run[B](silk:Silk[_], targetName:String) : Result[B] = {
    import ProgramTree._
    findTarget(silk, targetName) map { t =>
      run(t).asInstanceOf[Result[B]]
    } getOrElse ( throw new IllegalArgumentException(s"target $targetName is not found"))
  }

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

    import xerial.silk.framework.ops._
    import helper._
    silk match {
      case RawSeq(id, fref, in) => in.cast
      case MapOp(id, fref, in, f, fe) =>
        run(in).map(e => eval(fwrap(f)(e))).cast
      case FlatMapOp(id, fref, in, f, fe) =>
        run(in).flatMap(e => evalSeq(fwrap(f)(e))).cast
      case FilterOp(id, fref, in, f, fe) =>
        run(in).filter(f).cast
      case ReduceOp(id, fref, in, f, fe) =>
        Seq(run(in).reduce(f)).cast
      case other =>
        //warn(s"unknown silk type: $silk")
        Seq.empty
    }

  }
}


trait InMemorySliceStorage extends SliceStorageComponent {
  self: SilkFramework =>

  val sliceStorage = new SliceStorageAPI with Guard {
    private val table = collection.mutable.Map[(UUID, Int), Slice[_]]()
    private val futureToResolve = collection.mutable.Map[(UUID, Int), Future[Slice[_]]]()

    private val infoTable = collection.mutable.Map[Silk[_], StageInfo]()
    private val sliceTable = collection.mutable.Map[(Silk[_], Int), Seq[_]]()

    def get(op: Silk[_], index: Int): Future[Slice[_]] = guard {
      val key = (op.id, index)
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

    def poke(op:Silk[_], index:Int) : Unit = guard {
      val key = (op.id, index)
      if(futureToResolve.contains(key)) {
        futureToResolve(key).set(null)
        futureToResolve -= key
      }
    }

    def put(op: Silk[_], index: Int, slice: Slice[_], data:Seq[_]) {
      guard {
        val key = (op.id, index)
        if (!table.contains(key)) {
          table += key -> slice
          sliceTable += (op, index) -> data
        }
        if (futureToResolve.contains(key)) {
          futureToResolve(key).set(slice)
          futureToResolve -= key
        }
      }
    }

    def putRaw(op: InMemorySliceStorage.this.type#Silk[_], index: Int, slice: Slice[_], data: Array[Byte]) {
      SilkException.NA
    }

    def contains(op: Silk[_], index: Int) = guard {
      val key = (op.id, index)
      table.contains(key)
    }
    def getStageInfo(op: Silk[_]) = guard {
      infoTable.get(op)
    }

    def setStageInfo(op: Silk[_], si: StageInfo) {
      guard {
        infoTable += op -> si
      }
    }

    def retrieve[A](op: Silk[A], slice: Slice[A]) = {
      val key = (op, slice.index)
      sliceTable(key).asInstanceOf[Seq[A]]
    }

  }


}


trait InMemorySliceExecutor
  extends InMemoryFramework
  with InMemorySliceStorage
  //with InMemoryStageManager
{

  // Uses a locally stored slice for evaluation
  type Slice[V] = LocalSlice[V]

  case class LocalSlice[A](index: Int, data: Result[A])

  def newSlice[A](op: Silk[_], index: Int, data: Seq[A]): Slice[A] = {
    LocalSlice(index, data)
  }
}


//trait InMemoryStageManager extends StageManagerComponent {
//  type StageManger = StageManagerImpl
//  val stageManager = new StageManagerImpl
//
//  class StageManagerImpl extends StageManagerAPI with Logger {
//    def abortStage[A](op: Silk[A]) {}
//    def isFinished[A](op: Silk[A]): Boolean = false
//    def startStage[A](op: Silk[A]) {
//      trace(s"[${op.idPrefix}] started stage")
//    }
//    def finishStage[A](op: Silk[A]) {
//      trace(s"[${op.idPrefix}] finished stage")
//    }
//  }
//
//}