//--------------------------------------
//
// SilkSample.scala
// Since: 2014/01/12 23:09
//
//--------------------------------------

package xerial.silk.cui

import xerial.silk.core.{Weaver, Silk}
import Silk._

/**
 * @author Taro L. Saito
 */
class SilkSample(weaver:Weaver) {

  def in(n:Int) = (0 until n).toSilk
}