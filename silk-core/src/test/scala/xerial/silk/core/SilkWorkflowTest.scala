//--------------------------------------
//
// SilkWorkflowTest.scala
// Since: 2013/05/17 12:36 PM
//
//--------------------------------------

package xerial.silk.core

import xerial.silk.util.SilkSpec
import java.io.{ObjectOutputStream, ByteArrayOutputStream}
import xerial.silk._

import Silk._


trait NestedLoop {

  val A = Silk.newSilk(Seq(1, 2, 3))
  val B = Silk.newSilk(Seq("x", "y"))

  def main = for(a <- A; b <- B) yield (a, b)

}

case class Person(id:Int, name:String, age:Int)

trait SamplePerson {
  def P = Silk.newSilk(Seq(Person(1, "Peter", 22), Person(2, "Yui", 10), Person(3, "Aina", 0)))
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

  def B = Silk.newSilk(Seq(Address(1, "xxx"), Address(1, "yyy"), Address(3, "zzz")))
  def join = P.naturalJoin(B)

}


trait SampleInput {

  def main = Silk.newSilk(Seq(1, 2, 3, 4))

}



trait NestedMixinExample {

  // Import SampleInput.main under the scope of a variable, sample
  val sample = mixin[SampleInput]

  def main = sample.main.map(_*2)
}


/**
 * @author Taro L. Saito
 */
class SilkWorkflowTest extends SilkSpec {


  implicit val env = SilkEnv.inMemoryEnv

  "Workflow" should {

    "evaluate nested loops" taggedAs("nested") in {
      val w = workflow[NestedLoop]
      val g = CallGraph.createCallGraph(w.main)
      debug(w.fc)
      debug(g)
      debug(s"eval: ${w.main.get}")
    }

    "sequential operation" taggedAs("seq") in {
      val w = workflow[SeqOp]
      val g = CallGraph.createCallGraph(w.main)
      debug(g)
      debug(s"eval: ${w.main.get}")
    }

    "take joins" taggedAs("join") in {
      val w = workflow[Twig]
      val g = CallGraph.createCallGraph(w.join)
      debug(g)
      debug(s"eval : ${w.join.get}")

    }


    "allow nested mixin workflows" taggedAs("mixin") in {
      val w = workflow[NestedMixinExample]

      debug(s"w.sample.main owner: ${w.sample.main.fc.owner}")

      val g = CallGraph.createCallGraph(w.main)
      debug(g)

      debug(s"eval: ${w.main.get}")
    }

    "create new workflows" in {
      val w1 = workflow[NestedLoop]
      val w2 = workflow[NestedLoop]

      val g1 = CallGraph.createCallGraph(w1.main)
      debug(g1)
      val g2 = CallGraph.createCallGraph(w2.main)
      debug(g2)

    }


  }
}


object ExampleWorkflow {


 trait InputData {
   def input = Silk.loadFile("sample.txt") // Read a file
   def inputLines = input.lines
   def tsv = inputLines.map(_.split("\t")) // Split by tab
 }

 class MyTask1 extends InputData {
   def lineCount = inputLines.size
 }

 class MyTask2 extends InputData {
   def column1 = tsv.map(l => l(1)) // Select column 1

 }


 class ExtendedTask extends MyTask1 {
   override def inputLines = super.inputLines.takeSample(0.01) // Sampling 1%
 }

 class Mixin {
   val t1 = mixin[MyTask1]
   val t2 = mixin[ExtendedTask]

   def report = {
     //println(s"t1:${t1.lineCount.get}, t2:${t2.lineCount.get}")
   }
 }

}