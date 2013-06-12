//--------------------------------------
//
// DistributedCache.scala
// Since: 2013/06/11 15:26
//
//--------------------------------------

package xerial.silk.cluster.framework

import xerial.silk.framework.{SessionComponent, DistributedFramework, CacheComponent}
import xerial.silk.cluster.{ZkPath, ZooKeeperClient}
import xerial.silk.mini.SilkMini

/**
 * Distributed cache implementation based on zookeeper
 * @author Taro L. Saito
 */
trait DistributedCache extends CacheComponent {
  self: ZooKeeperService =>

  type Cache = DistributedCacheImpl
  val cache = new DistributedCacheImpl

  import xerial.silk.cluster.config

  class DistributedCacheImpl extends CacheAPI {

    def zkPathOf(path:String) = {
      config.zk.cachePath / path
    }
    def getOrElseUpdate(path: String, data: => Array[Byte]) = {
      val p = zkPathOf(path)
      if(zk.exists(p))
        zk.get(p).get
      else {
        zk.set(p, data)
        data
      }
    }

    def update(path: String, data: Array[Byte]) {
      zk.set(zkPathOf(path), data)
    }
    def remove(path: String) {
      zk.remove(zkPathOf(path))
    }
    def clear(path:String) {
      zk.remove(zkPathOf(path))
    }
  }
}


