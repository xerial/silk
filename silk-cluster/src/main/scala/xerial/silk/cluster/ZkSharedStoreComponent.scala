//--------------------------------------
//
// ZkSharedStoreComponent.scala
// Since: 2014/01/05 0:00
//
//--------------------------------------

package xerial.silk.cluster

import xerial.silk.framework.SharedStoreComponent

/**
 * SharedStore implementation using ZooKeeper
 *
 * @author Taro L. Saito
 */
trait ZkSharedStoreComponent extends SharedStoreComponent {

  self: ZooKeeperService =>

  type Store = ZkSharedStore
  val store = new ZkSharedStore

  class ZkSharedStore extends SharedStore {
    import ZkPath._
    def exist(path: String) = zk.exists(path.toZkPath)
    def get(path: String) = zk.get(path)
    def apply(path: String) = zk.read(path.toZkPath)
    def update(path: String, data: Array[Byte]) = zk.set(path.toZkPath, data)
    def remove(path: String) = zk.remove(path.toZkPath)
    def ls(path: String) = zk.ls(path)
  }

}