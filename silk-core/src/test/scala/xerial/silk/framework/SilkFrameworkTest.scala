//--------------------------------------
//
// SilkFrameworkTest.scala
// Since: 2013/06/09 12:15
//
//--------------------------------------

package xerial.silk.framework

import xerial.silk.util.SilkSpec
import xerial.core.log.Logger
import xerial.silk.Silk
import xerial.silk.framework.ops.{CallGraph, FilterOp, MapOp}


trait WorkWithParam {

  val factor: Int

  def in = Silk.newSilk(Seq(1, 2, 3, 4, 5, 6))
  def main = in.map(_ * factor)
}

class TestCode {
  def in = Silk.newSilk(Seq(1, 2, 3))
  def a = in.map(_ * 2).filter(_ % 2 == 0)
}

class LoopTest {
  def x = Silk.newSilk(Seq(1, 2))
  def y = Silk.newSilk(Seq("a", "b"))
  def a = for (v <- x; w <- y) yield {
    s"${v}:${w}"
  }
}

class CommandTest {
  import xerial.silk._

  def inputFiles = c"ls".lines
  def fileTypes = for(file <- inputFiles) yield c"file ${file}".string

  def echo = c"echo ${fileTypes}"
}

/**
 * @author Taro L. Saito
 */
class SilkFrameworkTest extends SilkSpec {

  before {
    Silk.setEnv(new InMemoryEnv)
  }

  "SilkFramework" should {

    "have in-memory runner" in {
      val in = Silk.newSilk(Seq(1, 2, 3, 4, 5, 6))
      val op = in.map(_ * 2).filter(_ < 10).reduce(_ + _)
      val result = op.get
      result shouldBe 20
    }

    "evaluate partial operation" in {
      val in = Silk.newSilk(Seq(1, 2, 3, 4, 5, 6))
      val a = in.map(_ * 2)
      val b = a.filter(_ < 10)
      val c = b.reduce(_ + _)

      val result = c.get("a")
      result shouldBe Seq(2, 4, 6, 8, 10, 12)
    }

    "allow tuning parameter set" in {
      val w1 = new WorkWithParam {
        val factor = 2
      }

      val w2 = new WorkWithParam {
        val factor = 3
      }
      w1.main.get shouldBe Seq(2, 4, 6, 8, 10, 12)
      w2.main.get shouldBe Seq(3, 6, 9, 12, 15, 18)
    }

    "resolve function ref" in {
      trait A {
        def mul(i: Int) = i * 2
        val in = Silk.newSilk(Seq(1, 2, 3, 4, 5, 6))
        val op = in.map(mul)
      }

      val a = new A {}
      val m = a.op.asInstanceOf[MapOp[_, _]]
      info(m)
    }

    "track a chained expression" in {
      val t = new TestCode
      val filter = t.a.asInstanceOf[FilterOp[_]]
      val mapOp = filter.in.asInstanceOf[MapOp[_, _]]
      info(filter)
      info(mapOp)
    }

    "track nested loops" in {
      val t = new LoopTest
      val a = t.a
      val g = CallGraph(a)
      info(g)
    }

    "track command execution" in {
      val t = new CommandTest
      val g = CallGraph(t.fileTypes)
      info(g)
      val result = t.fileTypes.get
      info(result)
    }

    "track command args" taggedAs("args") in {
      val t = new CommandTest
      t.echo.inputs
      val g = CallGraph(t.echo)
      info(g)
    }

    //    "have Silk splitter" taggedAs("split") in {
    //      val f = new SliceFramework
    //      val in = f.newSilk(Seq(1, 2, 3, 4, 5, 6))
    //      val op = in.map(_ * 2).filter(_ < 10).reduce(_ + _)
    //      val result = f.run(op)
    //    }


  }
}