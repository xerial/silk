//--------------------------------------
//
// SilkWorkFlowTest.scala
// Since: 2012/12/05 3:36 PM
//
//--------------------------------------

package xerial.silk.collection

import xerial.silk.util.SilkSpec

/**
 * @author Taro L. Saito
 */
class SilkWorkFlowTest extends SilkSpec {

  "Flow" should {
    "create worlflow graph" in {
      val f = SilkWorkFlow.newWorkflow("root", InMemorySilk(Seq(1, 2, 3, 4)))
      val f2 = f.map(_ * 2)

      debug(f2)
      debug(f2.eval)

      val s = SilkSerializer.serialize(f2)


      val f2_d = SilkSerializer.deserialize(s)

      debug(f2_d)
      debug(f2_d.eval)
    }


    case class Person(id:Int, name:String)

    "inspect variables used in function" in {
      val f = SilkWorkFlow.newWorkflow("root", InMemorySilk(Seq(Person(1, "leo"), Person(2, "yui"))))

      val prefix = "Hello "

      val f2 = f.map(prefix + _.name)

      SilkSerializer.checkClosure(f2.f)

      debug("serializing %s", f2.getClass)

      val ff = SilkSerializer.serializeClosure(f2.f)
      val ff_d = SilkSerializer.deserializeClosure(ff)

      debug(f.map(ff_d.asInstanceOf[Person => String]).eval)
    }

    "detect object access" in {
      val f = SilkWorkFlow.newWorkflow("root", InMemorySilk(Seq(Person(1, "leo"), Person(2, "yui"))))
      val f2 = f.map(p => if(p.id < 5) p.name else "N/A")
      val f3 = f.map(p => p.name)
      val accessed_in_f2 = SilkSerializer.accessedFields(classOf[Person], f2.f)
      val accessed_in_f3 = SilkSerializer.accessedFields(classOf[Person], f3.f)
      accessed_in_f2 should be (Seq("id", "name"))
      accessed_in_f3 should be (Seq("name"))
    }

  }

}