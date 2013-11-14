//--------------------------------------
//
// ProgramTreeTest.scala
// Since: 2013/04/22 15:19
//
//--------------------------------------

package xerial.silk.cluster

import xerial.silk.util.SilkSpec
import xerial.larray.LArray
import java.util.UUID
import xerial.silk.cluster.ProgramTree

/**
 * @author Taro L. Saito
 */
class ProgramTreeTest extends SilkSpec {

  import ProgramTree._

  "ProgramTree" should {
    "build trees" in {

      def newData = Data(UUID.randomUUID())
      val fastq = newData
      val scatter = ScatterNode(fastq, Seq(newData, newData))
      val f1 = new MapNode("readFASTQ", scatter, newData)
      val f2 = new MapNode("readFASTQ", scatter, newData)
      val m1 = new MapNode("align", f1, newData)
      val m2 = new MapNode("align", f2, newData)

      val g = GatherNode(Seq(m1, m2), newData)

      info(g)
    }




  }


}