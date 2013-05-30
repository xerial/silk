//--------------------------------------
//
// SilkMiniTest.scala
// Since: 2013/05/17 12:36 PM
//
//--------------------------------------

package xerial.silk.mini

import xerial.silk.util.SilkSpec
import xerial.silk.MacroUtil
import xerial.core.log.Logger

object SilkMiniTest {

  val sc = new SilkContext()

  def A = sc.newSilk(Seq(1, 2, 3))
  def B = sc.newSilk(Seq("x", "y"))

  def main = for(a <- A; b <- B) yield (a, b)

}

case class Person(id:Int, name:String, age:Int)

object SeqOp extends Logger {

  val sc = new SilkContext
  def P = sc.newSilk(Seq(Person(1, "Peter", 22), Person(1, "Yui", 10), Person(2, "Aina", 0)))

  def main = {
    val B = P.filter(_.age <= 20)
    val C = B.map(_.name)
    C
  }
}



/**
 * @author Taro L. Saito
 */
class SilkMiniTest extends SilkSpec {


  "SilkMini" should {

    "construct program" in {
      val op = SilkMiniTest.main
      debug(s"eval: ${op.eval}")
      debug(s"sc:\n${SilkMiniTest.sc}")
      //debug(s"eval again: ${main.eval}")
      val g = SilkMini.createCallGraph(op)
      debug(g)
    }

    "sequential operation" taggedAs("seq") in {
      val op = SeqOp.main
      debug(s"op:$op")
      debug(s"eval: ${op.eval}")

      val g = SilkMini.createCallGraph(op)
      debug(g)

    }


  }
}