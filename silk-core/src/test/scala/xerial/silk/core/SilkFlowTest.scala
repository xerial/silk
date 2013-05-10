//--------------------------------------
//
// SilkFlowTest.scala
// Since: 2013/05/09 6:00 PM
//
//--------------------------------------

package xerial.silk.core

import xerial.silk._
import xerial.silk.util.SilkSpec
import xerial.silk.core.SilkFlow.{MapFun, RawInput, SingleInput}

/**
 * @author Taro L. Saito
 */
class SilkFlowTest extends SilkSpec {
  "SilkFlow" should {

    "extract expression tree" in {
      val s = RawInput(Seq(1, 2))
      val m = s.map( _ * 2 )
      val expr = m.asInstanceOf[MapFun[_, _]].fExpr
      debug(expr)
    }

    "extract command string argument exprs" in {

      val ref = "hg19.fasta"
      val option = Seq("-a", "sw")
      val c : ShellCommand = c"bwa index ${option.mkString(",")} $ref"

      import scala.reflect.runtime.universe._
      debug(c.argsExpr.map(showRaw(_)))
    }
  }
}