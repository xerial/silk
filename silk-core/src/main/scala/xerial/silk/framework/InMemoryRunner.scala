package xerial.silk.framework

import xerial.core.log.Logger
import xerial.silk.mini.SilkMini
import scala.language.experimental.macros

import scala.reflect.ClassTag

trait InMemoryFramework extends SilkFramework {
  type Result[V] = Seq[V]

  def newSilk[A](in: Result[A])(implicit ev: ClassTag[A]): Silk[A] = macro SilkMini.newSilkImpl[A]

  protected def fwrap[A,B](f:A=>B) = f.asInstanceOf[Any=>Any]
  protected def filterWrap[A](f:A=>Boolean) = f.asInstanceOf[Any=>Boolean]
  protected def rwrap[P, Q, R](f: (P, Q) => R) = f.asInstanceOf[(Any, Any) => Any]

  protected def eval(v:Any) : Any = {
    v match {
      case s:Silk[_] => run(s)
      case other => other
    }
  }

  protected def evalSeq(seq:Any) : Seq[Any] = {
    seq match {
      case s:Silk[_] => run(s)
      case other => other.asInstanceOf[Seq[Any]]
    }
  }

}


/**
 * Silk runner for processing in-memory data
 * @author Taro L. Saito
 */
trait InMemoryRunner extends InMemoryFramework with Logger {

  def run[A](silk:Silk[A]) : Result[A] = {
    implicit class Cast(v:Any) {
      def cast : Result[A] = v.asInstanceOf[Result[A]]
    }

    import xerial.silk.mini._
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
