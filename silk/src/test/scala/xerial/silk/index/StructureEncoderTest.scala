//--------------------------------------
//
// StructureEncoderTest.scala
// Since: 2013/01/17 11:08 AM
//
//--------------------------------------

package xerial.silk.index

import xerial.silk.util.SilkSpec

object StructureEncoderTest {
  case class Person(id:Int, name:String)
  case class Employee(id:Int, name:String, address:Seq[Address])
  case class Manager(id:Int, name:String, title:String, address:Seq[Address])
  case class Address(address:String, phone:Option[String])
}


/**
 * @author Taro L. Saito
 */
class StructureEncoderTest extends SilkSpec {

  import StructureEncoderTest._

  val person = Person(1, "leo")
  val manager = Manager(2, "yui", "CEO", Seq(Address("1-2-3 XXX Street", Some("111-2222"))))
  val emp = Employee(3, "aina", Seq(Address("X-Y-Z Avenue", Some("222-3333")), Address("ABC State", None)))
  val emp2 = Employee(4, "silk", Seq(Address("Q Town", None)))
  val emp3 = Employee(5, "sam", Seq(Address("Windy Street", Some("999-9999"))))

  "StructureEncoder" should {
    "produce OrdPath and value pairs" in {
      val e = new StructureEncoder
      e.encode(person)
    }

    "encode objects containing Seq" taggedAs("seq") in {
      val e = new StructureEncoder
      e.encode(manager)
    }

    "encode mixed types" taggedAs("mixed") in {
      val e = new StructureEncoder
      e.encode(Seq(person, emp, manager, emp2))
      e.encode(Seq(emp3))
    }

  }

}