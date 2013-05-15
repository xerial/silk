//--------------------------------------
//
// SimpleExecutor.scala
// Since: 2013/05/15 10:38
//
//--------------------------------------

package xerial.silk.cluster.scheduler

import xerial.silk.core.{CallGraph, Silk, SilkSingle, SilkExecutor}
import xerial.silk.SilkException
import xerial.core.log.Logger

/**
 * @author Taro L. Saito
 */
class SimpleExecutor extends SilkExecutor with Logger {
  def eval[A](in: Silk[A]) = throw SilkException.pending

  def evalSingle[A](in: SilkSingle[A]) = {

    val g = CallGraph(in)
    debug(g)

    throw SilkException.pending
  }
}