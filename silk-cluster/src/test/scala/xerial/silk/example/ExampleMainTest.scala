//--------------------------------------
//
// ExampleMainTest.scala
// Since: 2012/12/21 1:28 PM
//
//--------------------------------------

package xerial.silk.example

import xerial.silk.util.SilkSpec
import xerial.silk.core.SilkSerializer

/**
 * @author Taro L. Saito
 */
class ExampleMainTest extends SilkSpec {
  "ExampleMain" should {
    "be serialized" in {

      val e = new ExampleMain
      val s = SilkSerializer.serializeObj(e)
      val d = SilkSerializer.deserializeObj(s)

    }


  }
}