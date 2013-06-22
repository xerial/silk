//--------------------------------------
//
// DistributedCache.scala
// Since: 2013/06/11 15:26
//
//--------------------------------------

package xerial.silk.cluster.framework

import xerial.silk.framework._
import xerial.silk.cluster.{ZkPath, ZooKeeperClient}
import java.io.{ByteArrayInputStream, ObjectInputStream}
import com.netflix.curator.framework.api.CuratorWatcher
import org.apache.zookeeper.WatchedEvent
import org.apache.zookeeper.Watcher.Event.EventType
import xerial.core.log.Logger
import xerial.silk.util.Guard
import xerial.silk.core.SilkSerializer

/**
 * Distributed cache implementation based on zookeeper
 * @author Taro L. Saito
 */
trait DistributedCache extends CacheComponent {
  self: ZooKeeperService =>

  type Cache = DistributedCacheImpl
  val cache = new DistributedCacheImpl

  import xerial.silk.cluster.config

  class DistributedCacheImpl extends CacheAPI with Logger {

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
    def get(path:String) : Option[Array[Byte]] = {
      zk.get(zkPathOf(path))
    }

    def contains(path:String) : Boolean = {
      zk.exists(zkPathOf(path))
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

    def getOrAwait(path:String) : SilkFuture[Array[Byte]] = {
      val p = zkPathOf(path)
      new SilkFuture[Array[Byte]] with CuratorWatcher with Guard { self =>
        val isReady = newCondition
        var isExists = zk.curatorFramework.checkExists().usingWatcher(self).forPath(p.path)

        def respond(k: (Array[Byte]) => Unit) {
          guard {
            if(isExists == null)
              isReady.await
          }
          k(zk.read(p))
        }

        def process(event: WatchedEvent) {
          def notify = guard {
              isExists = zk.curatorFramework.checkExists().forPath(p.path)
              isReady.signalAll()
          }

          event.getType match {
            case EventType.NodeCreated => notify
            case EventType.NodeDataChanged => notify
            case other => warn("unhandled event type: $other")
          }
        }
      }
    }


  }
}


trait DistributedSliceStorage extends SliceStorageComponent {
  self: SilkFramework with DistributedCache =>

  val sliceStorage = new SliceStorage

  class SliceStorage extends SliceStorageAPI {

    def slicePath(op:Silk[_], index:Int) = {
      // TODO append session path: s"${session.sessionIDPrefix}/slice/${op.idPrefix}/${index}"
      s"slice/${op.idPrefix}/${index}"
    }

    def get(op: Silk[_], index: Int) : Future[Slice[_]] = {
      val p = slicePath(op, index)
      cache.getOrAwait(p).map(b => SilkSerializer.deserializeObj(b).asInstanceOf[Slice[_]])
    }

    def put(op: Silk[_], index: Int, slice: Slice[_]) {
      cache.update(slicePath(op, index), SilkSerializer.serializeObj(slice))
    }
    def contains(op: Silk[_], index: Int) : Boolean = {
      cache.contains(slicePath(op, index))
    }
  }


}


