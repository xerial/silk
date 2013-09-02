package xerial.silk.framework

import scala.language.experimental.macros
import scala.language.higherKinds

import scala.reflect.ClassTag
import java.util.UUID
import xerial.silk.util.Guard
import xerial.core.log.Logger
import xerial.silk._
import xerial.silk.framework.ops.{FContext, RawSeq, SilkMacros}
import xerial.core.util.Shell
import xerial.silk.framework.ops.FContext
import xerial.silk.framework.ops.RawSeq


class InMemoryEnv extends SilkEnv {

  val service = new InMemoryFramework
    with InMemoryRunner
    with InMemorySliceStorage

  def eval[A](op:Silk[A]) {
    service.eval(op)
  }

  def run[A](op: Silk[A]): Seq[A] = {
    service.run(op)
  }

  def run[A](op: Silk[A], target: String): Seq[_] = {
    service.run(op, target)
  }

  private[silk] def sendToRemote[A](seq: RawSeq[A], numSplit: Int) = {
    //service.sliceStorage.put(seq.id, )
    seq
  }
  private[silk] def runF0[R](locality: Seq[String], f: => R) = {
    f
  }
}

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
trait InMemoryRunner extends InMemoryFramework with ProgramTreeComponent with Logger {

  private val resultTable = collection.mutable.Map[(SilkSession, FContext), Seq[_]]()
  private val defaultSession = SilkSession.defaultSession


  def run[B](silk: Silk[_], targetName: String): Result[B] = {
    import ProgramTree._
    findTarget(silk, targetName) map {
      t =>
        run(t).asInstanceOf[Result[B]]
    } getOrElse (throw new IllegalArgumentException(s"target $targetName is not found"))
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


    resultTable.getOrElseUpdate((defaultSession, silk.fc), {
      import xerial.silk.framework.ops._
      import helper._
      trace(s"run $silk")
      silk match {
        case RawSeq(id, fref, in) => in
        case RawSmallSeq(id, fref, in) => in
        case MapOp(id, fref, in, f) =>
          run(in).map(e => eval(fwrap(f)(e)))
        case FlatMapOp(id, fref, in, f) =>
          run(in).flatMap {
            e =>
              val app = fwrap(f)(e)
              val result = evalSeq(app)
              result
          }
        case FilterOp(id, fref, in, f) =>
          run(in).filter(f.asInstanceOf[Any=>Boolean])
        case ReduceOp(id, fref, in, f) =>
          Seq(run(in).reduce(f.asInstanceOf[(Any, Any) => Any]))
        case c@CommandOutputStringOp(id, fref, sc, args) =>
          val cmd = c.cmdString
          val result = Seq(scala.sys.process.Process(cmd).!!)
          result
        case c@CommandOutputLinesOp(id, fref, sc, args) =>
          val cmd = c.cmdString
          val pb = Shell.prepareProcessBuilder(cmd, inheritIO = false)
          val result = scala.sys.process.Process(pb).lines.toIndexedSeq
          result
        case other =>
          warn(s"unknown silk type: $silk")
          Seq.empty
      }
    }
    ).cast
  }
}


trait InMemorySliceStorage extends SliceStorageComponent with IDUtil {
  self: SilkFramework =>

  val sliceStorage = new SliceStorageAPI with Guard {
    private val table = collection.mutable.Map[(UUID, Int), Slice]()
    private val futureToResolve = collection.mutable.Map[(UUID, Int), SilkFuture[Slice]]()

    private val infoTable = collection.mutable.Map[Silk[_], StageInfo]()
    private val sliceTable = collection.mutable.Map[(UUID, Int), Seq[_]]()

    def get(opid: UUID, index: Int): SilkFuture[Slice] = guard {
      val key = (opid, index)
      if (futureToResolve.contains(key)) {
        futureToResolve(key)
      }
      else {
        val f = new SilkFutureMultiThread[Slice]
        if (table.contains(key)) {
          f.set(table(key))
        }
        else
          futureToResolve += key -> f
        f
      }
    }

    def poke(opid: UUID, index: Int): Unit = guard {
      val key = (opid, index)
      if (futureToResolve.contains(key)) {
        futureToResolve(key).set(null)
        futureToResolve -= key
      }
    }

    def put(opid: UUID, index: Int, slice: Slice, data: Seq[_]) {
      guard {
        val key = (opid, index)
        if (!table.contains(key)) {
          table += key -> slice
          sliceTable += (opid, index) -> data
        }
        if (futureToResolve.contains(key)) {
          futureToResolve(key).set(slice)
          futureToResolve -= key
        }
      }
    }

    def putRaw(opid: UUID, index: Int, slice: Slice, data: Array[Byte]) {
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

    def retrieve(opid: UUID, slice: Slice) = {
      val key = (opid, slice.index)
      sliceTable(key).asInstanceOf[Seq[_]]
    }
    def poke(opid: UUID, partition: Int, index: Int) {
      SilkException.NA
    }
    def putSlice(opid: UUID, partition: Int, index: Int, Slice: Slice, data: Seq[_]) {
      SilkException.NA
    }
    def getSlice(opid: UUID, partition: Int, index: Int) = {
      SilkException.NA
    }

  }


}


trait InMemorySliceExecutor
  extends InMemoryFramework
  with InMemorySliceStorage {
  //with InMemoryStageManager {

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