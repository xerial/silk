//--------------------------------------
//
// DAGSchedulerTest.scala
// Since: 2013/10/17 3:21 PM
//
//--------------------------------------

package xerial.silk.cluster.framework

import xerial.silk.cluster.{SilkCluster, Cluster2UserSpec, Cluster2Spec}
import xerial.silk.Silk

/**
 * @author Taro L. Saito
 */
class DAGSchedulerTestMultiJvm1 extends Cluster2Spec {

  "execute dependent task" in {
    start { env =>

    }
  }
}

class DAGSchedulerTestMultiJvm2 extends Cluster2UserSpec {

  after {
    SilkCluster.cleanUp
  }

  "execute dependent task" in {
    start { zkAddr =>
      implicit val env = SilkCluster.init(zkAddr)

      val a = Silk.newSilk(Seq(0, 1, 2))
      val b = a.map(_ * 2)
      val result = b.get

      debug(s"result: ${result.mkString(",")}")
    }
  }
}
