//--------------------------------------
//
// SilkFlowTest.scala
// Since: 2013/05/09 6:00 PM
//
//--------------------------------------

package xerial.silk.core

import xerial.silk._
import xerial.silk.util.SilkSpec
import core.SilkFlow.{Filter, MapFun, RawInput, SingleInput}

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
      val g = CallGraph(this.getClass, m)
      debug(s"call graph:\n$g")
    }

    "extract command string argument exprs" in {

      val ref = "hg19.fasta"
      val option = Seq("-a", "sw")
      val cmd : ShellCommand = c"bwa index ${option.mkString(",")} $ref"
      import scala.reflect.runtime.{universe=>ru}
      debug(cmd.argsExpr.map(ru.showRaw(_)))
    }

    "construct expression tree" in {
      val s = RawInput(Seq(1, 2))
      val e = s.map(_ * 2).map(_ - 1).filter(_ % 2 == 1)
      debug(e)
    }

    "create call graph from command pipeline" in {
      val g = CallGraph(SampleWorkflow.getClass, SampleWorkflow.align)
      debug(s"call graph:\n$g")
    }

    "join two Silks" in {
      val s = RawInput(Seq(1, 2))
      val t = RawInput(Seq(2, 3, 4))
      def id(v:Int) = v
      val r = s.join(t, id, id)
      val g = CallGraph(this.getClass, r)
      debug(s"call graph:\n$g")
    }

    "track val ref" in {
      val s = RawInput(Seq(1, 2))
      val mul = SingleInput(10)
      val r = for(s <- RawInput(Seq(1, 2)); mul <- SingleInput(10)) yield {
        s * mul
      }
      val g = CallGraph(this.getClass, r)
      debug(g)
    }
  }
}


object SampleWorkflow {

  val sampleName = "HA001"
  // Prepare fastq files
  def fastqFiles = c"""find $sampleName -name "*.fastq" """
  def ref = c"bwa index -a sw hg19.fa" as "hg19.fa"
  // alignment
  def align = {
    for{
      fastq  <- fastqFiles.lines
      saIndex <- c"bwa align -t 8 $ref $fastq".file
      sam <- c"bwa samse -P $ref $saIndex $fastq".file
    }
    yield
      sam
  }

}
