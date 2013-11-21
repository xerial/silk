//--------------------------------------
//
// ZooKeeperService.scala
// Since: 2013/06/11 15:30
//
//--------------------------------------

package xerial.silk.cluster

import com.netflix.curator.framework.CuratorFrameworkFactory
import com.netflix.curator.retry.ExponentialBackoffRetry
import xerial.silk.io.{ServiceGuard, MissingService}

/**
 * Zookeeper Service interface
 *
 * @author Taro L. Saito
 */
trait ZooKeeperService {
  val zk : ZooKeeperClient

//  object zookeeperService {
//
//    def newZooKeeperConnection : ZooKeeperClient = {
//      val cf = CuratorFrameworkFactory.newClient(zkConnectString, config.zk.clientSessionTimeout, config.zk.clientConnectionTimeout, config.zk.retryPolicy)
//      val c = new ZooKeeperClient(cf)
//      c
//    }
//
//    /**
//     * Get a ZooKeeper client. It will retry connection to the server the number of times specified by config.zk.clientConnectionMaxRetry.
//     *
//     * @return connection wrapper that can be used in for-comprehension
//     */
//    def defaultZkClient : ServiceGuard[ZooKeeperClient]  = ZooKeeper.zkClient(config.zk, zkConnectString)
//
//    def isAvailable : Boolean = ZooKeeper.isAvailable(zkServers)
//  }

}


