//--------------------------------------
//
// MakeTest.scala
// Since: 2013/07/31 2:32 PM
//
//--------------------------------------

package xerial.silk.weaver.example

import xerial.silk.util.SilkSpec
import xerial.silk.framework.ops.CallGraph
import xerial.silk.framework.InMemoryEnv
import xerial.silk.example.MakeExample


/**
 * @author Taro L. Saito
 */
class MakeTest extends SilkSpec {

  import xerial.silk._

  before {
    Silk.setEnv(new InMemoryEnv)
  }

  "Make example" should {

    "produce logical plan" in {
      val p = new AlignmentPipeline() {
        override def fastqFiles = Seq("sample.fastq").toSilk
      }
      val g = CallGraph(p.sortedBam)
      info(g)

    }

    "count lines of files a folder" in {
      val m = new MakeExample
      val g = CallGraph(m.lineCount)
      info(g)

      val result = m.lineCount.get
      info(s"line count result: $result")
    }

  }
}