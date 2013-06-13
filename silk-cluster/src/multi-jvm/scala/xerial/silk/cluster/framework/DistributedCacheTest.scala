//--------------------------------------
//
// DistributedCacheTest.scala
// Since: 2013/06/13 10:44
//
//--------------------------------------

package xerial.silk.cluster.framework

import xerial.silk.cluster.Cluster3Spec

object DistributedCacheTest {

  def test1 = "Distributed cache should monitor changes"
}

import DistributedCacheTest._

class DistributedCacheTestMultiJvm1 extends Cluster3Spec {

  test1 in {
    start { env =>

    }
  }
}


class DistributedCacheTestMultiJvm2 extends Cluster3Spec {

  test1 in {
    start { env => }
  }
}

class DistributedCacheTestMultiJvm3 extends Cluster3Spec {
  test1 in {
    start { env => }
  }
}