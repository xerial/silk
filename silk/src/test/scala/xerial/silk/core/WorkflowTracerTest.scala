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
      import xerial.silk.example.Align

      val a = new Align
      debug(a.ref)
      val dep = WorkflowTracer.traceMethodFlow(a.getClass, "ref")
      debug(s"dependency ${dep.get}")

      val dep2 = WorkflowTracer.traceMethodFlow(a.getClass, "align")
      debug(s"dependency ${dep2.get}")
    }
  }


}