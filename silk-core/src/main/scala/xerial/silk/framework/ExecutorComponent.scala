package xerial.silk.framework

import xerial.silk.mini._
import xerial.silk.SilkException
import xerial.silk.framework.ops.{ReduceOp, MapOp, FlatMapOp, FilterOp}

/**
 * Executor of the Silk program
 */
trait ExecutorComponent {
  self : SessionComponent
    with SliceComponent
    with StageManagerComponent
    with SliceStorageComponent =>

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
