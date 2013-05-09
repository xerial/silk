//--------------------------------------
//
// CmdStringTest.scala
// Since: 2013/04/16 19:11
//
//--------------------------------------

package xerial.silk.core

import xerial.silk.util.SilkSpec
import xerial.silk.core.SilkFlow.ShellCommand

object CmdStringTest {
  implicit class CmdBuilder(val sc:StringContext) extends AnyVal {
    def c(args:Any*) : ShellCommand = {
      new ShellCommand(sc, args:_*)
    }
  }
}



/**
 * @author Taro L. Saito
 */
class CmdStringTest extends SilkSpec {

  import CmdStringTest._
  
  "CmdString" should {
    "split template and arguments" in {
      val ref = "hg19"
      val fastq = "input.fastq"

      val cmd = c"bwa align $ref $fastq"

      debug(s"cmd template: ${cmd.templateString}")

      cmd.cmdString shouldBe (s"bwa align $ref $fastq")
      cmd.arg(0).toString shouldBe (ref)
      cmd.arg(1).toString shouldBe (fastq)
    }
  }
}