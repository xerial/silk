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
class SortTestMultiJvm1 extends Cluster3Spec {
  "send sort program" in {
    start { env =>

    }
  }
}

class SortTestMultiJvm2 extends Cluster3Spec {
  "send sort program" in {
    start { env =>

    }
  }
}

class SortTestMultiJvm3 extends ClusterUser3Spec {

  "send sort program" in {
    start {
      val s = new Sort()
      s.run.get
    }
  }

}