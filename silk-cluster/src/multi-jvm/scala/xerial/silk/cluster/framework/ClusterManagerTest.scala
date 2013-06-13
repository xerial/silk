//--------------------------------------
//
// ClusterManagerTest.scala
// Since: 2013/06/13 14:26
//
//--------------------------------------

package xerial.silk.cluster.framework

import xerial.silk.cluster.Cluster3Spec

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