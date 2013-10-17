//--------------------------------------
//
// CmdStringTest.scala
// Since: 2013/04/16 19:11
//
//--------------------------------------

package xerial.silk.core

import xerial.silk.util.SilkSpec
import xerial.silk._


/**
 * @author Taro L. Saito
 */
class CmdStringTest extends SilkSpec {

  "CmdString" should {
    "split template and arguments" in {
      val ref = "hg19"
      val fastq = "input.fastq"

      val cmd = c"bwa align $ref $fastq"

      debug(s"cmd template: ${cmd.cmdString}")
      //debug(s"arg exprs:\n${cmd.argsExpr.mkString("\n")}")


      cmd.cmdString shouldBe (s"bwa align $ref $fastq")
      cmd.arg(0).toString shouldBe (ref)
      cmd.arg(1).toString shouldBe (fastq)
    }

    "extract command arg" in {

      def ref = "ref"
      val s = c"hello $ref"
      val lines = c"hello $ref"
      //info(s"arg exprs: ${lines.argsExpr}")
      info(s"command line: ${lines.cmdString}")

      info(s"context: ${s}")
      info(s"context: ${lines}")
    }

  }
}

