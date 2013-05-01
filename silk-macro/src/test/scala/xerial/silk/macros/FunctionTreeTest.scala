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
      def square(v:Int) : Int = v * v
      val r = new SilkIntSeq(Seq(1, 2, 3))
      val result = for(v <- r) yield twice(v)
      debug(result.expr)

      debug(result.tree)


      val nested = for{
        v <- r
      } yield twice(square(v))
      debug(nested.tree)

    }

  }



}