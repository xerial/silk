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

    "accept CallGraph" in {
      implicit val s = new SimpleExecutor

      import xerial.silk._

      // Simple Silk program
      val input = (for(i <- 1 to 10) yield i).toSilk
      val r = input.map(_*2).sum

      val sum = r.get

      sum shouldBe 110
    }

  }
}