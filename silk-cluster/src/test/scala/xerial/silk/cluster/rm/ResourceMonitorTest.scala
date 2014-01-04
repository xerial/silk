//--------------------------------------
//
// ResourceMonitorTest.scala
// Since: 2014/01/04 18:38
//
//--------------------------------------

package xerial.silk.cluster.rm

import xerial.silk.util.SilkSpec
import xerial.silk.cluster.{DefaultLocalInfoComponent, LocalInfoComponent, ClusterWeaver}
import xerial.silk.framework.{LocalActorServiceComponent, InMemorySharedStoreComponent}

/**
 * @author Taro L. Saito
 */
class ResourceMonitorTest extends SilkSpec {

  "ResourceMonitor" should {
    "monitor actual node resources" in {

      val rm = new ResourceMonitorComponent
        with InMemorySharedStoreComponent
        with LocalActorServiceComponent
        with DefaultLocalInfoComponent
        with ClusterWeaver
      rm.start {
        val rs = rm.resourceTable.get
        info(s"resource state: $rs")
      }
    }

  }
}