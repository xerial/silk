//--------------------------------------
//
// MakeTest.scala
// Since: 2013/04/16 11:49 AM
//
//--------------------------------------

package xerial.silk.example

import xerial.silk.core.{MethodRef, Function2Ref, WorkflowTracer}
import xerial.silk.core.SilkWorkflow.{SilkFlow, FlowMap}
import xerial.lens.TypeUtil
import xerial.silk.multijvm.Cluster2Spec


/**
 * @author Taro L. Saito
 */
class MakeTestMultiJvm1 extends Cluster2Spec {
  "make should run unix commands" in {
    start { cli =>

      debug(s"flow: ${Make.md5sumAll}")
      val dep = WorkflowTracer.traceSilkFlow(Make.getClass, "md5sumAll")
      debug(s"dependency: $dep")

      for(d <- dep; m <- d.method) {
        val dd = WorkflowTracer.generateSilkFlow(m.cl, m.name)
        debug(s"flow: $dd")
      }

      // md5sumAll := Map(Cmd("find *.scala"), md5sum)
      // md5sum := Map(Cmd("md5sum ${}") , f)

    }
  }
}


class MakeTestMultiJvm2 extends Cluster2Spec {
  "make should run unix commands" in {
    start { cli =>

    }
  }
}