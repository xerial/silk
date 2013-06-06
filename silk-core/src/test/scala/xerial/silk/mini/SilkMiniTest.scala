//--------------------------------------
//
// SilkMiniTest.scala
// Since: 2013/05/17 12:36 PM
//
//--------------------------------------

package xerial.silk.mini

import xerial.silk.util.SilkSpec
import xerial.core.log.Logger

import mini._

object NestedLoop {

  def A = newSilk(Seq(1, 2, 3))
  def B = newSilk(Seq("x", "y"))

  def main = for(a <- A; b <- B) yield (a, b)

}

case class Person(id:Int, name:String, age:Int)

object SeqOp extends Logger {

  def P = newSilk(Seq(Person(1, "Peter", 22), Person(2, "Yui", 10), Person(3, "Aina", 0)))

  def main = {
    val B = P.filter(_.age <= 20)
    val C = B.map(_.name)
    C
  }
}

case class Address(id:Int, addr:String)

object Twig {

  def A = newSilk(Seq(Person(1, "Peter", 22), Person(2, "Yui", 10), Person(3, "Aina", 0)))
  def B = newSilk(Seq(Address(1, "xxx"), Address(1, "yyy"), Address(3, "zzz")))

  def join = A.naturalJoin(B)

}



/**
 * @author Taro L. Saito
 */
class SilkMiniTest extends SilkSpec {


  "SilkMini" should {

    "construct program" in {

      val ss = new SilkSession
      val op = NestedLoop.main

      debug(s"eval: ${op.eval(ss)}")
      debug(s"ss:\n${ss}")
      //debug(s"eval again: ${main.eval}")
      val g = SilkMini.createCallGraph(op)
      debug(g)
    }

    "sequential operation" taggedAs("seq") in {
      val ss = new SilkSession
      val op = SeqOp.main


      val g = SilkMini.createCallGraph(op)
      debug(g)

      debug(s"eval: ${op.eval(ss)}")
    }

    "take joins" taggedAs("join") in {
      val ss = new SilkSession
      val op = Twig.join
      val g = SilkMini.createCallGraph(op)
      debug(g)
      debug(s"eval : ${op.eval(ss)}")

    }


  }
}