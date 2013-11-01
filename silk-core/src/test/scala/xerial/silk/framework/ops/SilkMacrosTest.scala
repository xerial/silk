//--------------------------------------
//
// SilkMacrosTest.scala
// Since: 2013/11/01 9:14
//
//--------------------------------------

package xerial.silk.framework.ops

import xerial.silk.util.SilkSpec
import xerial.silk.{SilkSeq, SilkEnv, Silk}
import java.util.UUID

/**
 * @author Taro L. Saito
 */
class SilkMacrosTest extends SilkSpec {

  import Silk._

  implicit var env : SilkEnv = null

  before {
    env = Silk.testInit
  }

  after {
    Silk.cleanUp
  }

  def m(in:SilkSeq[Int]) = in.map(_+1)

  trait Sample {
    def in : SilkSeq[Int]
    def m = in.map(_*2)
  }

  "SilkMacros" should {
    "generate a different id for each operation" in {
      val s = Seq(1, 2, 3).toSilk
      val m1 = s.map(_*2)
      val m2 = s.map(_*10)
      debug(s"${m1.fc.refID}")
      debug(s"${m2.fc.refID}")
      m1.id should not be m2.id
    }

    "generate stable ids" in {
      val s = Seq(1, 2, 3).toSilk
      val s2 = Seq(1, 2, 3).toSilk
      // Produce the same ids for the same input
      val id1 = m(s).id
      val id2 = m(s).id
      id1 shouldBe id2

      // Produce a different id for a different input
      val id3 = m(s2).id
      id1 should not be id3
    }

    "generate different ids for code blocks" in {
      val s = Seq(1, 2, 3).toSilk

      val id1 = {
        val m = s.map(_*2)
        debug(s"fc:${m.fc.refID}")
        m.id
      }
      val id2 = {
        val m = s.map(_*2)
        debug(s"fc:${m.fc.refID}")
        m.id
      }

      id1 should not be id2
    }

    "generate different id for each trait instance" in {
      val s = Seq(10).toSilk
      val s1 = new Sample {
        def in = s
      }
      val s2 = new Sample {
        def in = s
      }

      debug(s1.m.fc.refID)
      debug(s2.m.fc.refID)
      debug(s1.m)
      debug(s2.m)
      s1.in.id shouldBe s2.in.id
      s1.m.id should not be s2.m.id
    }


  }
}