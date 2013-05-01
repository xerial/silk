//--------------------------------------
//
// FunctionTreeTest.scala
// Since: 2013/05/01 10:21 AM
//
//--------------------------------------

package xerial.silk.macros

object FunctionTreeTest {

  case class MapFun[A, B](f:A=>B)
}


/**
 * @author Taro L. Saito
 */
class FunctionTreeTest extends SilkMacroSpec {

  "FunctionTree" should {

    "retrieve function expr" in {
      def twice(v:Int) : Int = v * 2
      val r = new SilkIntSeq(Seq(1, 2, 3))
      r.map(twice)


    }

  }



}