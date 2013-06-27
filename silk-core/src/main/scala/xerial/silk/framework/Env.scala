//--------------------------------------
//
// Env.scala
// Since: 2013/06/25 3:56 PM
//
//--------------------------------------

package xerial.silk.framework

import xerial.silk.framework.ops.RawSeq

/**
 * @author Taro L. Saito
 */
trait SilkEnvLike extends Serializable {
  def sendToRemote[A](seq:RawSeq[A], numSplit:Int = 1) : Unit
}