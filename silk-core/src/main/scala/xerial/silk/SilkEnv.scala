package xerial.silk

import scala.reflect.ClassTag
import xerial.silk.framework.ops.{RawSeq, SilkMacros}
import scala.language.experimental.macros

/**
 * @author Taro L. Saito
 */
trait SilkEnv {
  def newSilk[A](in:Seq[A])(implicit ev:ClassTag[A]) : SilkSeq[A] = macro SilkMacros.newSilkImpl[A]
  def scatter[A](in:Seq[A], numSplit:Int)(implicit ev:ClassTag[A]) : SilkSeq[A] = macro SilkMacros.newSilkSplitImpl[A]

  def run[A](op:Silk[A]) : Seq[A]
  def run[A](op:Silk[A], target:String) : Seq[_]
  def eval[A](op:Silk[A]) : Unit
  private[silk] def sendToRemote[A](seq: RawSeq[A], numSplit:Int = 1) : RawSeq[A]


  private[silk] def runF0[R](locality:Seq[String], f: => R) : R
}
