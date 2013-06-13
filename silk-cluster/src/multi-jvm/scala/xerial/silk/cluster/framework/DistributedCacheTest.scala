//--------------------------------------
//
// DistributedCacheTest.scala
// Since: 2013/06/13 10:44
//
//--------------------------------------

package xerial.silk.cluster.framework

import xerial.silk.cluster.{Env, Cluster3Spec}

object DistributedCacheTest {

  def test1 = "Distributed cache should monitor changes"

  private[framework] def newCache(env:Env) = new DistributedCache with ZooKeeperService {
    val zk = env.zk
  }.cache

  val testPath = "hello"
  val testMessage = "Hello Distributed Cache!!"

}

import DistributedCacheTest._

class DistributedCacheTestMultiJvm1 extends Cluster3Spec {

  test1 in {
    start { env =>
      val cache = newCache(env)
      val future = cache.getOrAwait(testPath) map { b =>
        new String(b)
      }
      val s = future.get
      debug(s"read cache: $s")
      s shouldBe testMessage
    }
  }
}


class DistributedCacheTestMultiJvm2 extends Cluster3Spec {

  test1 in {
    start { env =>
      val cache = newCache(env)
      val future = cache.getOrAwait(testPath) map { b =>
        new String(b)
      }
      val s = future.get
      debug(s"read cache: $s")
      s shouldBe testMessage
    }
  }
}

class DistributedCacheTestMultiJvm3 extends Cluster3Spec {
  test1 in {
    start { env =>
      val cache = newCache(env)

      Thread.sleep(1000)
      debug(s"writing data")
      cache.update(testPath, testMessage.getBytes)
      cache.update(testPath, "next message".getBytes)

    }
  }
}