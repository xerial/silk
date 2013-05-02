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

  import ru._

  "FunctionTree" should {

    "retrieve function expr" in {
      val r = new SilkIntSeq(Seq(1, 2, 3))
      val result = for(v <- r) yield twice(v)
      trace(result.tree)
      val f = result.functionCall
      f.valName shouldBe "v"
    }

    "retrieve nested map" in {
      val r = new SilkIntSeq(Seq(1, 2, 3))
      val r1 = r.map(twice)
      trace(r1.tree)
      val fc1 = r1.functionCall
      fc1.valName shouldBe "v"

      val r2 = r1.map(square)
      trace(r2.tree)
      val fc2 = r2.functionCall
      fc2.valName shouldBe "w"
    }

    "find nested function call" in {
      val r = new SilkIntSeq(Seq(1, 2, 3))
      val r1 = r.map(v => square(twice(v)))
      trace(r1.tree)
      val fc1 = r1.functionCall
      trace(fc1)
      val calledMethods = FunctionTree.collectMethodCall(fc1.body)
      trace(showRaw(fc1.body))

      trace(calledMethods.mkString("\n"))
      calledMethods.size shouldBe 1
      val m1 = calledMethods.head
      m1.methodName shouldBe "square"

      val nestedMethodCalls = FunctionTree.collectMethodCall(m1.body)
      nestedMethodCalls.size shouldBe 1
      val m2 = nestedMethodCalls.head
      m2.methodName shouldBe "twice"
    }



  }



}