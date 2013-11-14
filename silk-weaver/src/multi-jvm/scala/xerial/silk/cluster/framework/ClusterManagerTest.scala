//--------------------------------------
//
// ClusterManagerTest.scala
// Since: 2013/06/13 14:26
//
//--------------------------------------

package xerial.silk.cluster.framework

import xerial.silk.cluster.{ZooKeeperService, Cluster3Spec}
import xerial.silk.cluster.rm.ClusterNodeManager

/**
 * @author Taro L. Saito
 */
object ClusterManagerTest {

  def listUpNodes = "ClusterManager should list up nodes"

}

import ClusterManagerTest._
import xerial.silk.cluster.Cluster3Spec


class ClusterManagerTestMultiJvm1 extends Cluster3Spec {
  listUpNodes in {
    start { env =>

      val m = new ClusterNodeManager with ZooKeeperService {
        val zk = env.zk
      }

      val activeNodes = m.nodeManager.nodes
      val nodeNames = activeNodes.map(_.name)
      nodeNames should (contain("jvm1"))
      nodeNames should (contain("jvm2"))
      nodeNames should (contain("jvm3"))
    }
  }
}

class ClusterManagerTestMultiJvm2 extends Cluster3Spec {
  listUpNodes in {
    start { env =>

    }
  }

}

class ClusterManagerTestMultiJvm3 extends Cluster3Spec {
  listUpNodes in {
    start { env =>

    }
  }

}