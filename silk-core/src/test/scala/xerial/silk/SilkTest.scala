//--------------------------------------
//
// SilkTest.scala
// Since: 2013/11/01 1:25 PM
//
//--------------------------------------

package xerial.silk

import xerial.silk.util.SilkSpec

/**
 * @author Taro L. Saito
 */
class SilkTest extends SilkSpec {

  import Silk._

  def hasDifferentIDs(in:Seq[Silk[_]]) = {
    in.combinations(2)
      .map(_.toList)
      .forall{ case List(a, b) => a.id != b.id}
  }


  "Silk" should {

    "have touch operation" in {

      val in = Seq(0, 1, 2).toSilk

      val iterative = for(i <- 1 to 2) yield {
        in.map(_ * i).touch
      }

      debug(iterative.mkString("\n"))
      hasDifferentIDs(iterative) should be (true)
    }

    "support iterative computing" in {

      val in = Seq(1, 2, 3).toSilk

      val lst = for(i <- 0 until 5) yield {
        val c = i.toSilkSingle
        debug(c)
        val m = in.mapWith(c){ case (c, x) => x + 1 }
        debug(m)
        m
      }

      hasDifferentIDs(lst) should be (true)
    }
  }
}