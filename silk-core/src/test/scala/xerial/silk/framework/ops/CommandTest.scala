//--------------------------------------
//
// CommandTest.scala
// Since: 2013/06/19 3:35 PM
//
//--------------------------------------

package xerial.silk.framework.ops

import xerial.silk.util.SilkSpec

/**
 * @author Taro L. Saito
 */
class CommandTest extends SilkSpec {

  "Command" should {
    "extract command arg" in {
      import xerial.silk._

      def ref = "ref"
      val s = c"hello $ref".toSilk
      val lines = c"hello $ref".lines
      info(s"arg exprs: ${lines.argsExpr}")
      info(s"command line: ${lines.cmdString}")

      info(s"context: ${s}")
      info(s"context: ${lines}")


    }

  }
}