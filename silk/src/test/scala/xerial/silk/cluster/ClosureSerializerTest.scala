//--------------------------------------
//
// ClosureSerializerTest.scala
// Since: 2013/04/03 2:56 PM
//
//--------------------------------------

package xerial.silk.cluster

import xerial.silk.util.SilkSpec

//object ClosureSerializerTest {
//
//  case class A(id:Int, name:String)
//}

/**
 * @author Taro L. Saito
 */
class ClosureSerializerTest extends SilkSpec {

//  import ClosureSerializerTest._

  def mylog = { warn("dive in deep") }

  "ClosureSerializer" should {
//    "detect accessed variables in nested functions" in {
//
//      def f(x:A) : Boolean = { x.id == 1 }
//      val accessedFields = ClosureSerializer.accessedFieldsInClosure(classOf[A], f)
//      accessedFields should contain ("id")
//    }


    "detect accsessed fields recursively" taggedAs("acc") in {

      //val accessedFields = ClosureSerializer.accessedFieldsIn(mylog)
      //debug(accessedFields)
      val ser = ClosureSerializer.serializeClosure(mylog)
      Remote.run(ser)
    }

  }
}