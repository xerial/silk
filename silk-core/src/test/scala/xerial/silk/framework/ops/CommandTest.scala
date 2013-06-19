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
      import Command._

      def ref = "ref"
      val s = c"hello $ref".toSilk
      val lines = c"hello $ref".lines
      info(s"arg exprs: ${lines.argsExpr}")
      info(s"command line: ${lines.cmdString}")

    }

  }
}