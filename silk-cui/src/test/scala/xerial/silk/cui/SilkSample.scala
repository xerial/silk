//--------------------------------------
//
// SilkSample.scala
// Since: 2014/01/12 23:09
//
//--------------------------------------

package xerial.silk.cui

import xerial.silk.Silk._
import xerial.silk.Weaver

/**
 * @author Taro L. Saito
 */
class SilkSample(weaver:Weaver) {

  def in(n:Int) = (0 until n).toSilk
}