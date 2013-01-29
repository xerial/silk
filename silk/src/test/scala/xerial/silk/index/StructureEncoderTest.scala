//--------------------------------------
//
// StructureEncoderTest.scala
// Since: 2013/01/17 11:08 AM
//
//--------------------------------------

package xerial.silk.index

import xerial.silk.util.SilkSpec
import xerial.lens.{SeqType, ObjectSchema, ObjectType}

object StructureEncoderTest {
  case class Person(id:Int, name:String)
  case class Employee(id:Int, name:String, address:Seq[Address])
  case class Manager(id:Int, name:String, title:String, address:Seq[Address])
  case class Address(line:String, phone:Option[String])
  case class Group(name:String, person:Seq[Employee])

  case class SeqSeq(name:String, seq1:Seq[String], seq2:Array[String])
}


/**
 * @author Taro L. Saito
 */
class StructureEncoderTest extends SilkSpec {

  import StructureEncoderTest._
  import StructureEncoder._

  val person = Person(1, "leo")
  val manager = Manager(2, "yui", "CEO", Seq(Address("1-2-3 XXX Street", Some("111-2222"))))
  val emp1 = Employee(3, "aina", Seq(Address("X-Y-Z Avenue", Some("222-3333")), Address("ABC State", None)))
  val emp2 = Employee(4, "silk", Seq(Address("Q Town", None)))
  val emp3 = Employee(5, "sam", Seq(Address("Windy Street", Some("999-9999"))))
  val manager2 = Manager(6, "kevin", "CTO", Seq(Address("QQQ Avenue", Some("134-4343"))))

  val g1emp1 = Employee(7, "lucy", Seq.empty)
  val g1emp2 = Employee(8, "nichole", Seq(Address("Japan", Some("3404-114333"))))

  "StructureEncoder" should {
    "produce OrdPath and value pairs" in {
      val e = simpleEncoder
      e.encode(person)
    }

    "encode objects containing Seq" taggedAs("seq") in {
      val e = simpleEncoder
      e.encode(manager)
    }

    "encode mixed types" taggedAs("mixed") in {

      val f = new SimpleFieldWriterFactory
      val e = new StructureEncoder(f)
      e.encode(Seq(person, emp1, manager, emp2))
      e.encode(Seq(emp3))
      e.encode(Group("group1", Seq(g1emp1, g1emp2)))

      debug(f.contentString)
    }

    "detect Seq type" taggedAs("seqtype") in {
      val s = Seq(emp1)
      debug(s.getClass)
      val t = ObjectType(s.getClass)
      debug(t)
      val schema = ObjectSchema(s.getClass)
      debug(schema)
    }

    "manage objects with multiple Seq types" taggedAs("seqseq") in {
      val f = new SimpleFieldWriterFactory
      val e = new StructureEncoder(f)
      e.encode(SeqSeq("test", Seq("A", "B"), Array("C", "D")))
      debug(f.contentString)

    }

    "detect Seq element type" taggedAs("elem") in {
      val schema = ObjectSchema(classOf[Employee])
      for(c <- schema.findConstructor; p <- c.findParameter("address")) {
        val t = p.valueType
        t match {
          case s:SeqType[_] => {
            debug("address type: %s", s)
            s.elementType.rawType should be (classOf[Address])
          }
          case _ => error("unexpected type: %s", t)
        }
      }
    }


  }

}