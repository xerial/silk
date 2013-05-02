//--------------------------------------
//
// FunctionTreeTest.scala
// Since: 2013/05/01 10:21 AM
//
//--------------------------------------

package xerial.silk.macros
import scala.reflect.runtime.{universe=>ru}
import sun.org.mozilla.javascript.internal.ast.FunctionCall

object FunctionTreeTest {

  case class MapFun[A, B](f:A=>B)
}


/**
 * @author Taro L. Saito
 */
class FunctionTreeTest extends SilkMacroSpec {

  def twice(v:Int) : Int = v * 2
  def square(w:Int) : Int = w * w

  "FunctionTree" should {

    "retrieve function expr" in {
      val r = new SilkIntSeq(Seq(1, 2, 3))
      val result = for(v <- r) yield twice(v)
      debug(result.tree)

      import ru._

      //debug(ru.showRaw(result.tree))

      val f = result.functionCall
      f.valName shouldBe "v"
    }

    "retrieve nested map" in {
      val r = new SilkIntSeq(Seq(1, 2, 3))
      val r1 = r.map(twice)
      debug(r1.tree)
      val fc1 = r1.functionCall
      fc1.valName shouldBe "v"

      val r2 = r1.map(square)
      debug(r2.tree)
      val fc2 = r2.functionCall
      fc2.valName shouldBe "w"


    }



  }



}