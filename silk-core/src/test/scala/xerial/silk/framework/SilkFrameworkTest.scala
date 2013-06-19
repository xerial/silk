//--------------------------------------
//
// SilkFrameworkTest.scala
// Since: 2013/06/09 12:15
//
//--------------------------------------

package xerial.silk.framework

import xerial.silk.util.SilkSpec
import xerial.core.log.Logger


trait RunLogger extends SilkRunner {

  abstract override def run[A](silk: Silk[A]) : Result[A] = {
    debug(s"run $silk")
    val result = super.run(silk)
    debug(s"result: $result")
    result
  }
}


class TestFramework extends InMemoryRunner


//class SliceFramework
//  extends InMemorySliceExecutor with RunLogger {
//
//  override def executor = new ExecutorImpl {
//    override def getSlices[A](v: Silk[A]) = {
//      debug(s"getSlices $v")
//      val result = super.getSlices(v)
//      debug(s"result: $result")
//      result
//    }
//  }
//}

trait WorkWithParam { this: InMemoryFramework =>

  val factor : Int

  def in = newSilk(Seq(1, 2, 3, 4, 5, 6))
  def main = in.map(_ * factor)
}


/**
 * @author Taro L. Saito
 */
class SilkFrameworkTest extends SilkSpec {
  "SilkFramework" should {

    "have in-memory runner" in {
      val f = new TestFramework
      val in = f.newSilk(Seq(1, 2, 3, 4, 5, 6))
      val op = in.map(_ * 2).filter(_ < 10).reduce(_ + _)
      val result = f.run(op)
      result shouldBe Seq(20)
    }

    "evaluate partial operation" in {
      val f = new TestFramework
      val in = f.newSilk(Seq(1, 2, 3, 4, 5, 6))
      val a = in.map(_ * 2)
      val b = a.filter(_ < 10)
      val c = b.reduce(_ + _)

      val result = f.run(c, "a")
      result shouldBe Seq(2, 4, 6, 8, 10, 12)
    }

    "allow tuning parameter set" in {
      val w1 = new TestFramework with WorkWithParam {
        val factor = 2
      }

      val w2 = new TestFramework with WorkWithParam {
        val factor = 3
      }

      w1.run(w1.main)
      w2.run(w2.main)
    }

    //    "have Silk splitter" taggedAs("split") in {
//      val f = new SliceFramework
//      val in = f.newSilk(Seq(1, 2, 3, 4, 5, 6))
//      val op = in.map(_ * 2).filter(_ < 10).reduce(_ + _)
//      val result = f.run(op)
//    }


  }
}