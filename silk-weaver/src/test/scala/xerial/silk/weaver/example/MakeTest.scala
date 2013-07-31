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

/**
 * @author Taro L. Saito
 */
class MakeTest extends SilkSpec {

  "Make example" should {
    "produce logical plan" in {
      val p = new Align()
      val g = CallGraph(p.sortedBam)
      info(g)
    }

  }
}