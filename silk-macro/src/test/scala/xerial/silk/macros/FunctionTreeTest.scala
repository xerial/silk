//--------------------------------------
//
// FunctionTreeTest.scala
// Since: 2013/05/01 10:21 AM
//
//--------------------------------------

package xerial.silk.macros
import scala.reflect.runtime.{universe=>ru}

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
      debug(result)

      import ru._

      debug(ru.showRaw(result.tree))

      result.tree.collect {
        case FunCall(fcall) =>
          // Function call
          debug(s"val:${fcall.valName}, body:${fcall.body}")
      }
    }

  }



}