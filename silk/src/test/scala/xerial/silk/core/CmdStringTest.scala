//--------------------------------------
//
// CmdStringTest.scala
// Since: 2013/04/16 19:11
//
//--------------------------------------

package xerial.silk.core

import xerial.silk.util.SilkSpec

/**
 * @author Taro L. Saito
 */
class CmdStringTest extends SilkSpec {

  import xerial.silk._

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