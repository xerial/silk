//--------------------------------------
//
// SilkCluster.scala
// Since: 2013/11/14 23:35
//
//--------------------------------------

package xerial.silk.cluster

import xerial.silk.cluster.store.DistributedCache
import xerial.silk.cluster.rm.ClusterNodeManager
import xerial.silk.framework.Node

/**
 * @author Taro L. Saito
 */
object SilkCluster {

  def hosts : Seq[Node] = {

    def collectClientInfo(zkc: ZooKeeperClient): Seq[Node] = {
      val cm = new ClusterNodeManager with ZooKeeperService {
        val zk = zkc
      }
      cm.nodeManager.nodes
    }

    val ci = ZooKeeper.defaultZkClient.flatMap(zk => collectClientInfo(zk))
    ci.toSeq
  }

  def master : Option[MasterRecord] = {
    def getMasterInfo(zkc: ZooKeeperClient) : Option[MasterRecord] = {
      val cm = new MasterRecordComponent  with ZooKeeperService with DistributedCache {
        val zk = zkc
      }
      cm.getMaster
    }
    ZooKeeper.defaultZkClient.flatMap(zk => getMasterInfo(zk)).headOption
  }

}