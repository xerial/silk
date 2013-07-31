//--------------------------------------
//
// MakeTest.scala
// Since: 2013/07/31 2:32 PM
//
//--------------------------------------

package xerial.silk.weaver.example

import xerial.silk.util.SilkSpec
import xerial.silk.example.Align
import xerial.silk.framework.ops.CallGraph
import xerial.silk.Silk
import xerial.silk.framework.InMemoryEnv
import xerial.silk.core.SilkFlowCallGraph

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
      val p = new Align() {
        override val fastqFiles = Seq("sample.fastq").toSilk
      }
      val g = CallGraph(p.sortedBam)
      info(g)

    }

  }
}