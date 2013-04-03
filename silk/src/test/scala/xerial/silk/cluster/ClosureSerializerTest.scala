//--------------------------------------
//
// ClosureSerializerTest.scala
// Since: 2013/04/03 2:56 PM
//
//--------------------------------------

package xerial.silk.cluster

import xerial.silk.util.SilkSpec

object ClosureSerializerTest {

  case class A(id:Int, name:String)
}

/**
 * @author Taro L. Saito
 */
class ClosureSerializerTest extends SilkSpec {

  import ClosureSerializerTest._

  "ClosureSerializer" should {
    "detect variable usage in nested functions" in {

      def f(x:A) : Boolean = { x.id == 1 }
      val accessedFields = ClosureSerializer.accessedFieldsInClosure(classOf[A], f)
      accessedFields should contain ("id")
    }

  }
}