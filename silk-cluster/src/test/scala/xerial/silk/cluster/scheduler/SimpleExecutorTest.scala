//--------------------------------------
//
// SimpleExecutorTest.scala
// Since: 2013/05/15 8:54
//
//--------------------------------------

package xerial.silk.cluster.scheduler

import xerial.silk.util.SilkSpec
import xerial.silk.core.CallGraph

/**
 * @author Taro L. Saito
 */
class SimpleExecutorTest extends SilkSpec {
  "SimpleExecutor" should {

    import xerial.silk._
    implicit val s = new SimpleExecutor

    "evaluate Silk" in {
      // Simple Silk program
      val input = (for(i <- 1 to 10) yield i).toSilk
      val r = input.map(_*2)

      r.sum.get shouldBe 110

      val ans = (for(i <- 1 to 10) yield i).map(_*2)
      r.toSeq shouldBe ans
    }

    "evaluate nested loops" in {
      val in = (for(i <- 0 until 10) yield i).toSilk
      val r = in.map(_ + 1).map(_ * 2)
      debug(r.toSeq)
    }

  }
}