//--------------------------------------
//
// SilkMiniTest.scala
// Since: 2013/05/17 12:36 PM
//
//--------------------------------------

package xerial.silk.mini

import xerial.silk.util.SilkSpec

/**
 * @author Taro L. Saito
 */
class SilkMiniTest extends SilkSpec {

  "SilkMini" should {

    "construct program" in {
      val sc = new SilkContext()
      val A = sc.newSilk(Seq("x", "y"))
      val B = sc.newSilk(Seq(1, 2, 3))
      val m = for(a <- A; b <- B) yield (a, b)
      debug(s"tree: $m")
      debug(s"eval: ${m.eval}")
      debug(s"sc:\n$sc")
    }


  }
}