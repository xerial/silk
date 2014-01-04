//--------------------------------------
//
// ResourceMonitorTest.scala
// Since: 2014/01/04 18:38
//
//--------------------------------------

package xerial.silk.cluster.rm

import xerial.silk.util.SilkSpec
import xerial.silk.cluster.{LocalInfoComponent, ClusterWeaver}
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
        with LocalInfoComponent
        with ClusterWeaver
      {
        def currentNodeName = "localhost"
      }

      rm.startup

      val rs = rm.resourceTable.get
      info(s"resource state: $rs")
    }

  }
}