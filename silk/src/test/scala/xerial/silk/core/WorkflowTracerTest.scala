//--------------------------------------
//
// WorkflowTracerTest.scala
// Since: 2013/04/23 3:25 PM
//
//--------------------------------------

package xerial.silk.core
import xerial.silk.util.SilkSpec
import xerial.silk.cluster.LazyF0


/**
 * @author Taro L. Saito
 */
class WorkflowTracerTest extends SilkSpec {
  "WorkflowTracer" should {
    "find method dependency" in {
      import xerial.silk.example.Align

      val a = new Align
      debug(a.ref)
      val dep = WorkflowTracer.traceSilkFlow(a.ref)
      debug(s"dependency ${dep.get}")


      debug(a.align)
      val dep2 = WorkflowTracer.traceSilkFlow(a.align)
      debug(s"dependency ${dep2.get}")

    }
  }


}