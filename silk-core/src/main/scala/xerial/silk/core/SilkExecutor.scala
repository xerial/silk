//--------------------------------------
//
// SilkExecutor.scala
// Since: 2013/05/15 9:05
//
//--------------------------------------

package xerial.silk.core

/**
 * @author Taro L. Saito
 */
trait SilkExecutor {
  def eval[A](in:Silk[A]) : Seq[A]
  def evalSingle[A](in:SilkSingle[A]) : A
}