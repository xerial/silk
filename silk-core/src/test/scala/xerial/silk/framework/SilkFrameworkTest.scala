//--------------------------------------
//
// SilkFrameworkTest.scala
// Since: 2013/06/09 12:15
//
//--------------------------------------

package xerial.silk.framework

import xerial.silk.util.SilkSpec
import xerial.silk.mini.{RawSeq, SilkMini}
import xerial.core.log.Logger

class TestFramework extends LocalFramework with Logger {
  override def run[A](silk: Silk[A]) = {
    debug(s"run $silk")
    val result = super.run(silk)
    debug(s"result: $result")
    result
  }
}


/**
 * @author Taro L. Saito
 */
class SilkFrameworkTest extends SilkSpec {
  "SilkFramework" should {

    "have in-memory cake" in {
      val f = new TestFramework
      val in = f.newSilk(Seq(1, 2, 3, 4, 5, 6))
      val op = in.map(_ * 2).filter(_ < 10).reduce(_ + _)
      val result = f.run(op)
      debug(s"result: $result")
    }

  }
}