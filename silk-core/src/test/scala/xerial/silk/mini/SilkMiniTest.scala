//--------------------------------------
//
// SilkMiniTest.scala
// Since: 2013/05/17 12:36 PM
//
//--------------------------------------

package xerial.silk.mini

import xerial.silk.util.SilkSpec
import xerial.silk.MacroUtil

object SilkMiniTest {

  val sc = new SilkContext()

  def A = sc.newSilk(Seq(1, 2, 3))
  def B = sc.newSilk(Seq("x", "y"))

  def main = for(a <- A; b <- B) yield (a, b)

}

/**
 * @author Taro L. Saito
 */
class SilkMiniTest extends SilkSpec {
  import SilkMiniTest._

  "SilkMini" should {

    "construct program" in {

      debug(s"eval: ${main.eval}")
      debug(s"sc:\n$sc")
      debug(s"eval again: ${main.eval}")

    }

  }
}