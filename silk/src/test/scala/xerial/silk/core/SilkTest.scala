//--------------------------------------
//
// SilkTest.scala
// Since: 2012/12/03 10:40 AM
//
//--------------------------------------

package xerial.silk.core

import xerial.silk.util.SilkSpec


/**
 * @author Taro L. Saito
 */
class SilkTest extends SilkSpec {

  "SilkInMemory" should {

    "support basic Seq operations" in {
      val s = SilkInMemory(Seq(0, 1, 2, 3, 4, 5, 6))
      val s2 = s.map(x => x*2)
      debug(s2.mkString(", "))
      s2.getClass should be (classOf[SilkInMemory[_]])
    }

    "support for loop with conditions" in {
      val s = SilkInMemory(Seq(0, 1, 2, 5, 34))
      val s2 = for(x <- s if x % 2 == 1) yield {
        x * 10
      }
      debug(s2.mkString(", "))
    }

    "support core operations" in {
      val s = SilkInMemory(Seq(0, 1, 2, 5, 34))
      val z = s.zipWithIndex
      debug(z.mkString(", "))
    }

  }
}