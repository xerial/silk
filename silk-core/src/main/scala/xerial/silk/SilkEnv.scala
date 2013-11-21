package xerial.silk

import xerial.silk.core.CallGraph
import SilkException.NA

/**
 * Defines a cluster environment to execute Silk operations
 * @author Taro L. Saito
 */
trait SilkEnv extends Serializable {

  def get[A](op:SilkSeq[A]) : Seq[A] = run(op).get
  def get[A](op:SilkSingle[A]) : A = run(op).get
  def get[A](silk:Silk[A], target:String) : Any = {
    CallGraph.findTarget(silk, target).map {
      case s:SilkSeq[_] => run(s)
      case s:SilkSingle[_] => run(s)
    } getOrElse { SilkException.error(s"target $target is not found in $silk") }
  }

  def run[A](op:SilkSeq[A]) : SilkFuture[Seq[A]] = NA
  def run[A](op:SilkSingle[A]) : SilkFuture[A] = NA
  private[silk] def runF0[R](locality:Seq[String], f: => R) : R = NA

}

