//--------------------------------------
//
// MakeTest.scala
// Since: 2013/04/16 11:49 AM
//
//--------------------------------------

package xerial.silk.example

import xerial.silk.cluster.Cluster2Spec

/**
 * @author Taro L. Saito
 */
class MakeTestMultiJvm1 extends Cluster2Spec {
  "make should run unix commands" in {
    start { cli =>
      Make.md5sumAll.run 
    }
  }
}


class MakeTestMultiJvm2 extends Cluster2Spec {
  "make should run unix commands" in {
    start { cli =>

    }
  }
}