//--------------------------------------
//
// DistributedCache.scala
// Since: 2013/06/11 15:26
//
//--------------------------------------

package xerial.silk.cluster.framework

import xerial.silk.framework._
import xerial.silk.cluster.{ZkPath, ZooKeeperClient}
import xerial.silk.mini.SilkMini
import java.io.{ByteArrayInputStream, ObjectInputStream}

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
    def get(path:String) : Array[Byte] = {
      zk.get(path).get
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

    def watch(path:String) = {
      zk.watch(zkPathOf(path))
    }
  }
}


trait DistributedSliceStorage extends SliceStorageComponent {
  self: SilkFramework with DistributedCache with SessionComponent =>

  type Future[A] = SilkFuture[A]

  val sliceStorage = new SliceStorage

  class SliceStorage extends SliceStorageAPI {

    private def deserializeSlice(b:Array[Byte]) : Slice[_] = {
      val oin = new ObjectInputStream(new ByteArrayInputStream(b))
      val in = oin.readObject()
      in.asInstanceOf[Slice[_]]
    }

    def slicePath(op:Silk[_], index:Int) = {
      s"${op.idPrefix}/slice/${index}"
    }

    def get(op: Silk[_], index: Int) : Future[Slice[_]] = {
      val p = slicePath(op, index)
      if(cache.contains(p)) {
        val b = cache.get(p)
        val slice = deserializeSlice(b)
        new ConcreteSilkFuture[Slice[_]](slice)
      }
      else {
        // TODO: Convert this blocking operation (watch) to non-blocking
        cache.watch(p)
      }
    }

    def put(op: Silk[_], index: Int, slice: Slice[_]) {}
    def contains(op: Silk[_], index: Int) = ???
  }


}


