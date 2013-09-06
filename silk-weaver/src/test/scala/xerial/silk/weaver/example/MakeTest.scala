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

  "MakeExample" should {

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

    "memorize computed results" taggedAs("memo") in {
      val w = new MakeExample
      val t1 = w.lineCount
      val t2 = w.lineCount

      debug(s"t1: $t1")
      debug(s"t2: $t2")
      val r1 = t1.get
      val r2 = t2.get
      debug(s"r1: $r1")
      (r1 eq r2) should be (true)
    }

    "allow running multiple workflows" taggedAs("mul") in {
      pending
      val w1 = Silk.registerWorkflow("w1", new MakeExample)
      val w2 = Silk.registerWorkflow("w2", new MakeExample)

      val r1 = w1.lineCount.get
      val r2 = w2.lineCount.get
      (r1 eq r2) should not be (true)
    }

  }
}