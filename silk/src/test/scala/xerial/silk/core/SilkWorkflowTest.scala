//--------------------------------------
//
// SilkWorkflowTest.scala
// Since: 2012/12/05 3:36 PM
//
//--------------------------------------

package xerial.silk.core

import xerial.silk.util.SilkSpec
import xerial.silk.cluster.ClosureSerializer._
import xerial.silk.cluster.ClosureSerializer
import xerial.silk.core.SilkWorkflow.SilkTask

/**
 * @author Taro L. Saito
 */
class SilkWorkflowTest extends SilkSpec {

  import SilkWorkflowTest._

  "Flow" should {


    "inspect variables used in function" in {


      val f = SilkWorkflow.newWorkflow("root", SilkInMemory(Seq(Person(1, "leo"), Person(2, "yui"))))

      val prefix = "Hello "

      val f2 = f.map(prefix + _.name)

      debug("serializing %s", f2.getClass)
      val ff = ClosureSerializer.serializeClosure(f2.f)
      val ff_d = ClosureSerializer.deserializeClosure(ff)
    }

    "detect object access" in {
      val f = SilkWorkflow.newWorkflow("root", SilkInMemory(Seq(Person(1, "leo"), Person(2, "yui"))))
      val f2 = f.map(p => if (p.id < 5) p.name else "N/A")
      val f3 = f.map(p => p.name)
      val accessed_in_f2 = accessedFields(classOf[Person], f2.f)
      val accessed_in_f3 = accessedFields(classOf[Person], f3.f)
      accessed_in_f2 should be(Seq("id", "name"))
      accessed_in_f3 should be(Seq("name"))
    }


    "serialize SilkFlow" taggedAs ("sflow") in {
      val p = new Person(0, "rookie")

      val pb = SilkSerializer.serialize(p)
      val p2 = SilkSerializer.deserializeAny(pb)
      debug("deserialized %s", p2)

      val seq = Seq(Person(1, "leo"), Person(2, "yui"))
      val sb = SilkSerializer.serialize(seq)
      val seq1 = SilkSerializer.deserializeAny(sb)
      debug("deserialized %s", seq1)

      val data = SilkInMemory(seq)
      val db = SilkSerializer.serialize(data)
      val d2 = SilkSerializer.deserializeAny(db)
      debug("deserialized %s", d2)


      val f = SilkWorkflow.newWorkflow("root", data)

      val b = SilkSerializer.serialize(f)
      //def printBinary = b.map(x => x.toChar).mkString.sliding(80, 80).mkString("\n")
      //debug("binary:\n%s", printBinary)
      val b2 = SilkSerializer.deserializeAny(b)
      debug(b2)
    }

    "construct workflow" in {
      import xerial.silk._

      val w = Seq(1, 2, 3).toFlow("my plan")
      val w2 = w.map(_ * 2).filter(_ >= 3)
      debug(s"workflow: $w2")

      // How do we extract the result from the plan?
    }


    def myTask = { xerial.macros.Macros.enclosingMethodName }

    "retrieve task name using macro" taggedAs("macro") in {



      debug("expr: %s", myTask)
      //myTask.printContext
      //myTask.name should be ("myTask")
    }


  }

}


object SilkWorkflowTest {
  case class Person(id: Int, name: String)
}

