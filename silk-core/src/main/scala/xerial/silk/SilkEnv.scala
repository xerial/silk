package xerial.silk

import xerial.silk.core.{MapOp, RawSeq, CallGraph}
import SilkException.NA
import scala.collection.GenTraversable
import scala.util.Random
import xerial.core.util.Shell
import scala.sys.process.Process
import xerial.core.log.Logger
import scala.io.Source
import xerial.lens.ConstructorParameter

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

  implicit class toFMapRes[A, B, C](f: (A, B) => GenTraversable[C]) {
    def toFmapRes = f.asInstanceOf[(Any, Any) => GenTraversable[Any]]
  }

}


object SilkEnv {

  import core._

  def inMemoryEnv : SilkEnv = new InMemoryExecutor()

}