//--------------------------------------
//
// SilkMacrosTest.scala
// Since: 2013/11/01 9:14
//
//--------------------------------------

package xerial.silk.framework.core

import xerial.silk.util.SilkSpec
import xerial.silk.{Weaver, SilkSeq, Silk}
import xerial.silk.core.Partitioner

/**
 * @author Taro L. Saito
 */
class SilkMacrosTest extends SilkSpec {

  import Silk._

  implicit var env : Weaver = Weaver.inMemoryWeaver

  def e[A](silk:Silk[A]) = {
    debug(silk)
    silk
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
      val s = new Sample {
        val in = Seq(1, 2, 3).toSilk
      }
      val id1 = s.in.id
      val id2 = s.in.id
      id1 shouldBe id2
    }

    "generate different ids for each function call" in {
      val s = new Sample {
        def in = Seq(1, 2, 3).toSilk
      }
      val id1 = s.in.id
      val id2 = s.in.id
      id1 should not be id2
    }

    "generate dependent ids" in {
      val s = Seq(1, 2, 3).toSilk
      // Produce different ids when a function is used to generate Silk
      val id1 = m(s).id
      val id2 = m(s).id
      id1 should not be id2

      // Produce a different id for a different input
      val s2 = Seq(1, 2, 3).toSilk
      val id3 = m(s2).id
      id1 should not be id3
      id2 should not be id3
    }

    "generate different ids for different code blocks" in {
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

    "generate different ids in filter core" in {
      val a = (0 until 10).toSilk
      a.filter(_ > 2).id should not be (a.filterNot(_ > 2).id)
    }


    "compile every operation" taggedAs("op") in {
      val a = (0 until 10).toSilk
      e(a)
      e(a.map(_*2))
      e(a.fMap(x => (0 until x).map(v => v)))
      e(a.filter(_ > 2))
      e(a.filterNot(_ > 2))
      e(a.head)
      e(a.takeSample(0.1))
      e(a.collect{ case i:Int if i % 2 == 0 => i })
      e(a.collectFirst{ case i:Int if i % 2 == 0 => i })
      e(a.distinct)

      val split = a.split
      e(split)
      e(split.concat)

      e(a.groupBy(_ % 2))
      e(a.aggregate(0)({case (sum, x) => sum + x}, {case (sum1, sum2) => sum1 + sum2}))

      val shuffle = a.shuffle(Partitioner({v:Int => v / 3}, 2))
      e(shuffle)
      e(shuffle.shuffleReduce)

      val v = 10.toSilkSingle
      e(v)
      e(a.mapWith(v){(x:Int, i:Int) => x * i})
      val w = 2.toSilkSingle
      e(w)
      e(a.mapWith(v, w){(x:Int, v:Int, w:Int) => x * v * w})
    }

  }
}