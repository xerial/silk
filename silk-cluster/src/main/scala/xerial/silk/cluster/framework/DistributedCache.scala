//--------------------------------------
//
// DistributedCache.scala
// Since: 2013/06/11 15:26
//
//--------------------------------------

package xerial.silk.cluster.framework

import xerial.silk.framework.CacheComponent
import xerial.silk.cluster.{ZkPath, ZooKeeperClient}
import xerial.silk.mini.SilkMini

/**
 * Distributed cache implementation based on zookeeper
 * @author Taro L. Saito
 */
trait DistributedCache extends CacheComponent with ZooKeeperService {
  type Cache = DistributedCacheImpl
  val cache = new DistributedCacheImpl

  import xerial.silk.cluster.config

  class DistributedCacheImpl extends CacheAPI {

    def statusPathFor[A](op:Silk[A]) = {
      ZkPath(s"${config.zk.cachePath}/${session.sessionIDPrefix}/${op.idPrefix}/status")
    }

    def getOrElseUpdate[A](op: Silk[A], result: ResultRef[A]) : ResultRef[A]= {
      val p = statusPathFor(op)
      val ref = zkClient.get(p) match {
        case Some(data) => SilkMini.deserializeObj(data).asInstanceOf[ResultRef[A]]
        case None => {
          zkClient.set(p, SilkMini.serializeObj(result))
          result
        }
      }
      ref
    }

    def update[A](op: Silk[A], result: ResultRef[A]) {
      zkClient.set(statusPathFor(op), SilkMini.serializeObj(result))
    }
    def remove[A](op: Silk[A]) {
      zkClient.remove(statusPathFor(op))
    }
    def clear {

    }

  }
}


