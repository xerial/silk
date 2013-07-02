//--------------------------------------
//
// WorkflowTest.scala
// Since: 2013/05/17 12:36 PM
//
//--------------------------------------

package xerial.silk.framework

import xerial.silk.util.SilkSpec
import xerial.core.log.Logger
import java.io.{ObjectOutputStream, ByteArrayOutputStream}
import xerial.silk.framework.ops.CallGraph
import xerial.silk._

trait NestedLoop {

  def A = Seq(1, 2, 3).toSilk
  def B = Seq("x", "y").toSilk

  def main = for(a <- A; b <- B) yield (a, b)

}

case class Person(id:Int, name:String, age:Int)

trait SamplePerson {
  def P = Seq(Person(1, "Peter", 22), Person(2, "Yui", 10), Person(3, "Aina", 0)).toSilk
}

trait SeqOp extends SamplePerson {

  def main = {
    val B = P.filter(_.age <= 20)
    val C = B.map(_.name)
    C
  }
}

case class Address(id:Int, addr:String)


trait Twig extends SamplePerson {

  def B = Seq(Address(1, "xxx"), Address(1, "yyy"), Address(3, "zzz")).toSilk
  def join = P.naturalJoin(B)

}


trait SampleInput {

  def main = Seq(1, 2, 3, 4).toSilk

}



trait NestedMixinExample {

  val sample = mixin[SampleInput]

  def main = sample.main.map(_*2)
}


/**
 * @author Taro L. Saito
 */
class WorkflowTest extends SilkSpec {


  before {
    info("before")
    Silk.setEnv(new InMemoryEnv)
  }

  after {
    info("after")
  }

  "Workflow" should {

    "evaluate nested loops" taggedAs("nested") in {
      val w = Workflow.of[NestedLoop]
      import w._
      val g = CallGraph.createCallGraph(w.main)
      debug(g)
      debug(s"eval: ${w.main.eval}")
    }

    "sequential operation" taggedAs("seq") in {
      val w = Workflow.of[SeqOp]
      import w._
      val g = CallGraph.createCallGraph(w.main)
      debug(g)
      debug(s"eval: ${w.main.eval}")
    }

    "take joins" taggedAs("join") in {
      val w = Workflow.of[Twig]
      import w._

      val g = CallGraph.createCallGraph(w.join)
      debug(g)
      debug(s"eval : ${w.join.eval}")

    }

    "serialize SilkSession" taggedAs("ser") in {
      val s = new SilkSession("default")

      val bo = new ByteArrayOutputStream
      val oos = new ObjectOutputStream(bo)
      oos.writeObject(s)
      oos.close

      val b = bo.toByteArray

      debug(s"serialized: ${b.length}")
    }

    "allow nested mixin workflows" taggedAs("mixin") in {
      val w = Workflow.of[NestedMixinExample]
      import w._

      debug(s"w.sample.main owner: ${w.sample.main.fc.owner}")

      val g = CallGraph.createCallGraph(w.main)
      debug(g)

      debug(s"eval: ${w.main.eval}")
    }


  }
}