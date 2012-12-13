//--------------------------------------
//
// ClusterManagerTest.scala
// Since: 2012/12/13 3:55 PM
//
//--------------------------------------

package xerial.silk.cluster

import xerial.silk.util.SilkSpec

/**
 * @author Taro L. Saito
 */
class ClusterManagerTest extends SilkSpec {

  "ClusterManager" should {
    "read $HOME/.silk/hosts file" in {
      val hosts = ClusterManager.defaultHosts()
      debug(hosts.mkString(", "))
    }

    "check the installation of Silk" in {
      val installed = ClusterManager.isSilkInstalled(MachineResource.localhost)
      debug("silk installation: %s", installed)
    }
  }
}