//--------------------------------------
//
// DistributedCache.scala
// Since: 2013/06/11 15:26
//
//--------------------------------------

package xerial.silk.cluster.framework

import xerial.silk.framework.CacheComponent
import xerial.silk.cluster.ZooKeeperClient

/**
 * Distributed cache implementation based on zookeeper
 * @author Taro L. Saito
 */
trait DistributedCache extends CacheComponent {
  type Cache = DistributedCacheImpl
  val cache = new DistributedCacheImpl

  val zkClient : ZooKeeperClient

  class DistributedCacheImpl extends CacheAPI {



    def getOrElseUpdate[A](op: Silk[A], result: ResultRef[A]) = ???
    def update[A](op: Silk[A], result: ResultRef[A]) {}
    def remove[A](op: Silk[A]) {}
    def clear {}
  }
}


