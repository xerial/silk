//--------------------------------------
//
// ResourceMasterTest.scala
// Since: 2014/01/05 0:34
//
//--------------------------------------

package xerial.silk.cluster.rm

import xerial.silk.util.SilkSpec
import xerial.silk.cluster.{DefaultLocalInfoComponent, LocalInfoComponent, ClusterWeaver}
import xerial.silk.framework.{NodeResourceRequest, ClusterResource, LocalActorServiceComponent, InMemorySharedStoreComponent}
import java.util.UUID
import xerial.core.util.DataUnit
import DataUnit._

/**
 * @author Taro L. Saito
 */
class ResourceMasterTest extends SilkSpec {
  "ResourceMaster" should {

    "hold resource table" in {

      val rm = new ResourceMasterComponent
        with ClusterWeaver
        with InMemorySharedStoreComponent
        with DefaultLocalInfoComponent
        with LocalActorServiceComponent
        with ResourceMonitorComponent

      rm.start {
        Thread.sleep(500)
        rm.resourceMaster.init

        rm.resourceMaster.ask(ClusterResource(UUID.randomUUID(), Seq(NodeResourceRequest("localhost", 1, 1 * KB))))

      }

    }

  }
}