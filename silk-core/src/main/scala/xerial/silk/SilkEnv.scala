package xerial.silk

import xerial.silk.core.{MapOp, RawSeq, CallGraph}
import SilkException.NA
import scala.collection.GenTraversable
import scala.util.Random
import xerial.core.util.Shell
import scala.sys.process.Process
import xerial.core.log.Logger

/**
 * Defines a cluster environment to execute Silk operations
 * @author Taro L. Saito
 */
trait SilkEnv extends Serializable {

  def get[A](op:SilkSeq[A]) : Seq[A] = run(op).get
  def get[A](op:SilkSingle[A]) : A = run(op).get
  def get[A](silk:Silk[A], target:String) : Any = {
    CallGraph.findTarget(silk, target).map {
      case s:SilkSeq[_] => run(s).get
      case s:SilkSingle[_] => run(s).get
    } getOrElse { SilkException.error(s"target $target is not found in $silk") }
  }

  def run[A](op:SilkSeq[A]) : SilkFuture[Seq[A]] = NA
  def run[A](op:SilkSingle[A]) : SilkFuture[A] = NA
  private[silk] def runF0[R](locality:Seq[String], f: => R) : R = NA

}

trait FunctionWrap {

  implicit class toGenFun[A, B](f: A => B) {
    def toF1: Any => Any = f.asInstanceOf[Any => Any]

    def toFlatMap: Any => SilkSeq[Any] = f.asInstanceOf[Any => SilkSeq[Any]]
    def tofMap: Any => GenTraversable[Any] = f.asInstanceOf[Any => GenTraversable[Any]]
    def toFilter: Any => Boolean = f.asInstanceOf[Any => Boolean]
  }

  implicit class toGenFMap[A, B](f: A => GenTraversable[B]) {
    def toFmap = f.asInstanceOf[Any => GenTraversable[Any]]
  }

  implicit class toAgg[A, B, C](f: (A, B) => C) {
    def toAgg = f.asInstanceOf[(Any, Any) => Any]
  }

}


object SilkEnv {

  import core._

  def inMemoryEnv : SilkEnv = new SilkEnv with FunctionWrap with Logger {

    private def future[A](v:A) : SilkFuture[A] = new ConcreteSilkFuture[A](v)


    override def run[A](op:SilkSeq[A]) : SilkFuture[Seq[A]] =
      future(eval(op).asInstanceOf[Seq[A]])


    override def run[A](op:SilkSingle[A]) : SilkFuture[A] = {
      future(eval(op).asInstanceOf[A])
    }

    private def eval(silk:SilkSeq[_]) : Seq[_] = {
      debug(s"eval $silk")
      silk match {
        case RawSeq(id, fc, seq) => seq
        case MapOp(id, fc, in, f) => eval(in).map(f.toF1)
        case MapFilterOp(id, fc, in, f, ft) =>
          eval(in).map(f.toF1).filter(ft.toFilter)
        case FlatMapFilterOp(id, fc, in, f, ft) =>
          eval(in).flatMap(f.tofMap).filter(ft.toFilter)
        //case FlatMapOp(id, fc, in, f) => eval(in).flatMap(f.tofMap)
        case FlatMapSeqOp(id, fc, in, f) => eval(in).flatMap(f.tofMap)
        case FilterOp(id, fc, in, f) => eval(in).filter(f.toFilter)
        case SplitOp(id, fc, in) => eval(in)
        case ConcatOp(id, fc, in) => NA
        case SortOp(id, fc, in, ord, partitioner) => eval(in).sorted(ord.asInstanceOf[Ordering[Any]])
        case GroupByOp(id, fc, in, f) => eval(in).groupBy(f.toF1).toSeq
        case SamplingOp(id, fc, in, p) => {
          val input = eval(in).toIndexedSeq
          val size = input.size
          val numSample : Int = math.min(size, math.min(1, math.floor(size.toDouble * p).toInt))
          for(i <- 0 until numSample) yield
            input(Random.nextInt(size))
        }
        case ZipWithIndexOp(id, fc, in) => eval(in).zipWithIndex
        case CollectOp(id, fc, in, pf) => eval(in).collect(pf.asInstanceOf[PartialFunction[Any, Any]])
        case DistinctOp(id, fc, in) => eval(in).distinct
        case cmd@CommandOutputLinesOp(id, fc, sc, args) => {
          val pb = Shell.prepareProcessBuilder(cmd.cmdString(this), true)
          Process(pb).lines
        }
        case Silk.Empty => Seq.empty
        case other => SilkException.error(s"unknown op:$other")
      }
    }

    private def eval(silk:SilkSingle[_]) : Any = {
      debug(s"eval ${silk}")
      silk match {
        case MapSingleOp(id, fc, in, f) => f.toF1(eval(in))
        //case FlatMapOp(id, fc, in, f) => eval(in).flatMap(f.tofMap)
        //case FilterSingleOp(id, fc, in, f) => f.toFilter.apply()
        case SizeOp(id, fc, in) => eval(in).size.toLong
        case ReduceOp(id, fc, in, f) =>
          eval(in).reduce(f.asInstanceOf[(Any, Any)=>Any])
        case AggregateOp(id, fc, in, z, seqop, combop) =>
          eval(in).aggregate[Any](z)(seqop.toAgg, combop.toAgg)
        case cmd@CommandOp(id, fc, sc, args, resource) =>
          Shell.exec(cmd.cmdString(this))
        case other => SilkException.error(s"unknown op: $other")
      }

    }

    override private[silk] def runF0[R](locality:Seq[String], f: => R) : R = {
      f
    }

  }

}