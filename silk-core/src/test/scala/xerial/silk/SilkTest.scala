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

  implicit var env : SilkEnv = null

  before {
    env = Silk.testInit
  }
  after {
    Silk.cleanUp
  }

  "Silk" should {

    "have touch operation" in {

      val in = Seq(0, 1, 2).toSilk

      val iterative = for(i <- 1 to 2) yield {
        in.map(_ * i).touch
      }

      debug(iterative.mkString("\n"))

      val idTest =
        iterative
          .combinations(2)
          .map(_.toList)
          .forall{ case List(a, b) => a.id != b.id }
      idTest should be (true)
    }
  }
}