//--------------------------------------
//
// DistributedCache.scala
// Since: 2013/06/11 15:26
//
//--------------------------------------

package xerial.silk.cluster.framework

import xerial.silk.framework._
import xerial.silk.cluster.{SilkClient, ZkPath, ZooKeeperClient}
import java.io.{ByteArrayInputStream, ObjectInputStream}
import com.netflix.curator.framework.api.CuratorWatcher
import org.apache.zookeeper.WatchedEvent
import org.apache.zookeeper.Watcher.Event.EventType
import xerial.core.log.Logger
import xerial.silk.util.Guard
import xerial.silk.core.SilkSerializer
import java.net.URL
import xerial.core.io.IOUtil
import xerial.silk.SilkException
import xerial.silk.cluster.DataServer.{MmapData, ByteData, RawData}
import xerial.larray.{MMapMode, LArray}

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
  self: SilkFramework with DistributedCache with NodeManagerComponent with LocalClientComponent =>

  type LocalClient = SilkClient
  val sliceStorage = new SliceStorage

  class SliceStorage extends SliceStorageAPI with Logger {

    private def slicePath(op:Silk[_], index:Int) = {
      // TODO append session path: s"${session.sessionIDPrefix}/slice/${op.idPrefix}/${index}"
      s"slice/${op.idPrefix}/${index}"
    }

    private def sliceInfoPath(op:Silk[_]) = {
      // TODO append session path: s"${session.sessionIDPrefix}/slice/${op.idPrefix}/${index}"
      s"slice/${op.idPrefix}/info"
    }

    def getSliceInfo(op:Silk[_]) : Option[SliceInfo] = {
      val p = sliceInfoPath(op)
      cache.get(p).map(b => SilkSerializer.deserializeObj[SliceInfo](b))
    }

    def setSliceInfo(op:Silk[_], sliceInfo:SliceInfo) {
      val p = sliceInfoPath(op)
      info(s"set slice info: $p")
      cache.update(p, SilkSerializer.serializeObj(sliceInfo))
    }

    def get(op: Silk[_], index: Int) : Future[Slice[_]] = {
      val p = slicePath(op, index)
      cache.getOrAwait(p).map(b => SilkSerializer.deserializeObj(b).asInstanceOf[Slice[_]])
    }

    def put(op: Silk[_], index: Int, slice: Slice[_], data:Seq[_]) {
      val path = s"${op.idPrefix}/${index}"
      debug(s"set data $path: ${data.mkString(", ")}")
      localClient.dataServer.registerData(path, data)
      cache.update(slicePath(op, index), SilkSerializer.serializeObj(slice))
    }

    def contains(op: Silk[_], index: Int) : Boolean = {
      cache.contains(slicePath(op, index))
    }

    def retrieve[A](op:Silk[A], slice: Slice[A]) = {
      val dataID = s"${op.idPrefix}/${slice.index}"
      if(slice.nodeName == localClient.currentNodeName) {
        SilkClient.client.flatMap { c =>
          c.dataServer.getData(dataID) map {
            case RawData(s, _) => s.asInstanceOf[Seq[_]]
            case ByteData(b, _) => SilkSerializer.deserializeObj[Seq[_]](b)
            case MmapData(file, _) => {
              val mmapped = LArray.mmap(file, 0, file.length, MMapMode.READ_ONLY)
              SilkSerializer.deserializeObj[Seq[A]](mmapped.toInputStream)
            }
          }
        } getOrElse { SilkException.error(s"no slice data is found: ${slice}") }
      }
      else {
        nodeManager.getNode(slice.nodeName).map { n =>
          val url = new URL(s"http://${n.address}:${n.dataServerPort}/data/${dataID}")
          debug(s"retrieve data from $url")
          IOUtil.readFully(url.openStream) { data =>
            SilkSerializer.deserializeObj[Seq[_]](data)
          }
        } getOrElse { SilkException.error(s"invalid node name: ${slice.nodeName}") }
      }
    }
  }


}


