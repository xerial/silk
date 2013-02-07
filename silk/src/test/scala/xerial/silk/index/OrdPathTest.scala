//--------------------------------------
//
// OrdPathTest.scala
// Since: 2013/01/17 9:57 AM
//
//--------------------------------------

package xerial.silk.index

import xerial.silk.util.SilkSpec

/**
 * @author Taro L. Saito
 */
class OrdPathTest extends SilkSpec {
  "OrdPath" should {
    "be comparable" in {
      OrdPath("1.2.5") should be (OrdPath("1.2.5"))
      OrdPath("1.3") should not be (OrdPath("1.3.4"))
    }

    "be constructed from strings" in {
      val p = OrdPath("1")
      p.length should be (1)
      p(0) should be (1)
      p.toString should be ("1")

      val p1 = OrdPath("1.1")
      p1.length should be (2)
      p1(0) should be (1)
      p1(1) should be (1)
      p1.toString should be ("1.1")

      val p2 = OrdPath("1.2.3")
      p2.length should be (3)
      p2(0) should be (1)
      p2(1) should be (2)
      p2(2) should be (3)
      p2.toString should be ("1.2.3")

      val c = p :+ 3
      c.length should be (2)
      c(0) should be (1)
      c(1) should be (3)
      c.toString should be ("1.3")


      p.parent should be (None)
      p1.parent.get should be (p)
      p2.parent.get should be (OrdPath("1.2"))
    }

    "have a child" in {
      OrdPath("1.2").child should be (OrdPath("1.2.0"))
    }

    "be incrementable" in {
      val p = OrdPath("1.2.4")
      p.next(0) should be (OrdPath("2"))
      p.next(1) should be (OrdPath("1.3"))
      p.next(2) should be (OrdPath("1.2.5"))
      p.next(3) should be (OrdPath("1.2.4.0"))
      p.next(4) should be (OrdPath("1.2.4.0.0"))
    }

    "take incremental diff" in {
      OrdPath("1.1.2").incrementalDiff(OrdPath("1.1.1")) should be (OrdPath("0.0.1"))
      OrdPath("2.2.1").incrementalDiff(OrdPath("1")) should be (OrdPath("1.2.1"))
      OrdPath("2.1.1").incrementalDiff(OrdPath("1")) should be (OrdPath("1.1.1"))
      OrdPath("2.1.1").incrementalDiff(OrdPath("1.1.1")) should be (OrdPath("1.1.1"))
      OrdPath("1.2.1").incrementalDiff(OrdPath("1.1.1")) should be (OrdPath("0.1.1"))
    }

  }
}