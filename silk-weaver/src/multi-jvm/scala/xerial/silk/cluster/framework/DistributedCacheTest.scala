//--------------------------------------
//
// DistributedCacheTest.scala
// Since: 2013/06/13 10:44
//
//--------------------------------------

package xerial.silk.cluster.framework

import xerial.silk.cluster._
import scala.util.Random
import xerial.silk.framework.{CacheAPI}
import xerial.silk.cluster.store.DistributedCache


object DistributedCacheTest {


  def futureTest = "Distributed cache should monitor changes"
  def durabilityTest = "Durability test"

  //private[framework] def newCache(client: SilkClientService) : CacheAPI = client.cache

  val testPath = "hello"
  val testMessage = "Hello Distributed Cache!!"
  val N = 1000

  def slicePath(i: Int, processID: Int) = s"slice/${processID}-$i"
  def sliceData(i: Int) = new Random(i).nextString(300).getBytes()
}

import DistributedCacheTest._

class DistributedCacheTestMultiJvm1 extends Cluster3Spec {

  futureTest in {
    start {
      env =>
        val cache = env.cache
        val future = cache.getOrAwait(testPath) map {
          b =>
            new String(b)
        }
        val s = future.get
        debug(s"read cache: $s")
        s shouldBe testMessage
    }
  }

  durabilityTest in {
    start {
      env =>
        val cache = env.cache

        debug("start writing data")
        for (i <- 0 until N)
          cache.update(slicePath(i, processID), sliceData(i))

        enterBarrier("done")

        debug("validating written data")
        def arrayEq(a: Array[Byte], b: Array[Byte]) = a.zip(b).forall(x => x._1 == x._2)

        val isValid = (0 until N).par.forall {
          i =>
            (1 to numProcesses).forall {
              p =>
                arrayEq(cache.get(slicePath(i, p)).get, sliceData(i))
            }
        }
        isValid should be(true)
    }
  }
}


class DistributedCacheTestMultiJvm2 extends Cluster3Spec {

  futureTest in {
    start {
      env =>
        val cache = env.cache
        val future = cache.getOrAwait(testPath) map {
          b =>
            new String(b)
        }
        val s = future.get
        debug(s"read cache: $s")
        s shouldBe testMessage
    }
  }

  durabilityTest in {
    start {
      env =>
        val cache = env.cache
        for (i <- 0 until N)
          cache.update(slicePath(i, processID), sliceData(i))

        enterBarrier("done")

    }

  }

}

class DistributedCacheTestMultiJvm3 extends Cluster3Spec {
  futureTest in {
    start {
      env =>
        val cache = env.cache

        Thread.sleep(1000)
        debug(s"writing data")
        cache.update(testPath, testMessage.getBytes)
        cache.update(testPath, "next message".getBytes)

    }
  }

  durabilityTest in {
    start {
      env =>
        val cache = env.cache
        for (i <- 0 until N)
          cache.update(slicePath(i, processID), sliceData(i))

        enterBarrier("done")
    }

  }

}