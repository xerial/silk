//--------------------------------------
//
// WorkflowTracerTest.scala
// Since: 2013/04/23 3:25 PM
//
//--------------------------------------

package xerial.silk.core
import xerial.silk.util.SilkSpec
import xerial.silk.cluster.LazyF0
import xerial.silk.core.SilkWorkflow.RootWrap

object SampleWork {

  import xerial.silk._

  def input = new RootWrap("input", Seq(0, 1, 2).toSilk)
  def a = input.map(mul)
  def inline = input.map(_*2)
  def factor = Silk.single(2)
  def mul(v:Int) = c"awk '{ print $v * $factor; }'"
}



/**
 * @author Taro L. Saito
 */
class WorkflowTracerTest extends SilkSpec {
  "WorkflowTracer" should {
    import xerial.silk.example.Align

    "find dependency in sample" taggedAs("sample") in {
      info(s"workflow: ${SampleWork.a}")
      val dep = WorkflowTracer.dependencyGraph(SampleWork.a)
      debug(s"dependency ${dep}")
    }

    "find dependency in inline map" taggedAs("map") in {
      info(s"workflow: ${SampleWork.inline}")
      val dep = WorkflowTracer.dependencyGraph(SampleWork.inline)
      debug(s"dependency ${dep}")
    }

    "find method dependency in ref" taggedAs("ref") in {
      val a = new Align
      debug(a.ref)
      val dep = WorkflowTracer.dependencyGraph(a.ref)
      debug(s"dependency ${dep}")
    }

    "find method dependency in align" taggedAs("align") in {
      val a = new Align
      debug(a.align)
      val dep2 = WorkflowTracer.dependencyGraph(a.align)
      debug(s"dependency ${dep2}")
    }

    "find method dependency in saIndex" taggedAs("saIndex") in {
      val a = new Align
      debug(a.saIndex _)
      val dep3 = WorkflowTracer.dependencyGraph(a.saIndex _)
      debug(s"dependency ${dep3}")

    }
  }


}