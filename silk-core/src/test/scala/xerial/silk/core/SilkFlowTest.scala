//--------------------------------------
//
// SilkFlowTest.scala
// Since: 2013/05/09 6:00 PM
//
//--------------------------------------

package xerial.silk.core

import xerial.silk._
import xerial.silk.util.SilkSpec
import xerial.silk.core.SilkFlow._
import xerial.silk.core.SilkFlow.MapFun
import xerial.silk.core.SilkFlow.RawInput

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
      val g = CallGraph(m)
      debug(s"call graph:\n$g")
    }

    "extract command string argument exprs" in {

      val ref = "hg19.fasta"
      val option = Seq("-a", "sw")
      val cmd = c"bwa index ${option.mkString(",")} $ref"
      import scala.reflect.runtime.{universe=>ru}
      debug(cmd.argsExpr.map(ru.showRaw(_)))
    }

    "construct expression tree" in {
      val s = RawInput(Seq(1, 2))
      val e = s.map(_ * 2).map(_ - 1).filter(_ % 2 == 1)
      debug(e)
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
      val mul = RawInputSingle(10)
      val r = for(s <- RawInput(Seq(1, 2)); m <- mul) yield {
        s * m
      }
      val g = CallGraph(r)
      debug(g)
    }



    "parse for comprehension" in {
      val m = for{e <- RawInput(Seq(1, 2));
                  x <- seq(e)} yield x * 2
      val g = CallGraph(m)
      debug(g)
    }

    "create call graph from command pipeline" in {
      val g = CallGraph(SampleWorkflow.align)
      debug(s"call graph:\n$g")
    }

    "nested operation" taggedAs("nested") in {
      pending
      implicit val ex = new SilkExecutor {
        def eval[A](in: Silk[A]) = throw SilkException.pending
        def evalSingle[A](in: SilkSingle[A]) = {
          in match {
            case RawInputSingle(v) => v
            case _ => throw SilkException.na
          }
        }
      }

      def a = RawInputSingle(3)
      val b = RawInputSingle(10)

      val m = for(e <- RawInput(Seq(1, 2))) yield {
        e * a.get * b.get
      }
      val g = CallGraph(m)
      debug(g)
    }
  }

  def seq(v:Int) = (for(i <- 0 until v) yield i).toSilk
}


object SampleWorkflow {

  val sampleName = "HA001"
  // Prepare fastq files
  // alignment

  def ref = c"bwa index -a sw hg19.fa" as "hg19.fa"

  def fastqFiles = c"""find $sampleName -name "*.fastq" """.lines

  def align = {
    for{
      fastq  <- fastqFiles
      saIndex <- c"bwa align -t 8 $ref $fastq".file
      sam <- c"bwa samse -P $ref $saIndex $fastq".file
    }
    yield
      sam
  }

}
