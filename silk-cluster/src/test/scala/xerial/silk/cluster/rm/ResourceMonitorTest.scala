//--------------------------------------
//
// ResourceMonitorTest.scala
// Since: 2014/01/04 18:38
//
//--------------------------------------

package xerial.silk.cluster.rm

import org.scalatest.FunSuite
import xerial.silk.util.SilkSpec
import xerial.silk.cluster.StandaloneCluster

/**
 * @author Taro L. Saito
 */
class ResourceMonitorTest extends SilkSpec {

  "ResourceMonitor" should {
    "monitor actual node resources" in {

      StandaloneCluster.withClusterAndClient{service =>
        val rs = service.resourceMonitor.get
        info(s"resource state: $rs")
      }

    }



  }
}