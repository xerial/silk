//--------------------------------------
//
// WorkflowTracerTest.scala
// Since: 2013/04/23 3:25 PM
//
//--------------------------------------

package xerial.silk.core
import xerial.silk.util.SilkSpec



/**
 * @author Taro L. Saito
 */
class WorkflowTracerTest extends SilkSpec {
  "WorkflowTracer" should {
    "find method dependency" in {
      import xerial.silk.example.Align._

      val a = new Align
      debug(a.ref)
      val dep = WorkflowTracer.traceSilkFlow("ref", a.ref)
      debug(s"dependency $dep")
    }
  }


}