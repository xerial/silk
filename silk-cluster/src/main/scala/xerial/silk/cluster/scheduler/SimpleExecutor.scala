//--------------------------------------
//
// SimpleExecutor.scala
// Since: 2013/05/15 10:38
//
//--------------------------------------

package xerial.silk.cluster.scheduler

import xerial.silk.core.{Silk, SilkSingle, SilkExecutor}
import xerial.silk.SilkException

/**
 * @author Taro L. Saito
 */
class SimpleExecutor extends SilkExecutor {
  def eval[A](in: Silk[A]) = throw SilkException.pending

  def evalSingle[A](in: SilkSingle[A]) = throw SilkException.pending
}