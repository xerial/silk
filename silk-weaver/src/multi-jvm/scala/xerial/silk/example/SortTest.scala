//--------------------------------------
//
// SortTest.scala
// Since: 2013/07/26 10:33 AM
//
//--------------------------------------

package xerial.silk.example

import xerial.silk.cluster._

/**
 * @author Taro L. Saito
 */
class SortTestMultiJvm1 extends Cluster2Spec {
  "send sort program" in {
    start { env =>

    }
  }
}

class SortTestMultiJvm2 extends ClusterUser2Spec {

  "send sort program" in {
    start { zkAddr =>
      val ex = new ExampleMain
      ex.sort(zkConnectString = zkAddr, N=64 * 1024, M=2, numReducer = 1)
    }
  }

}