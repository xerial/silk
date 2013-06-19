//--------------------------------------
//
// ZooKeeperService.scala
// Since: 2013/06/11 15:30
//
//--------------------------------------

package xerial.silk.cluster.framework

import xerial.silk.cluster._
import xerial.silk.framework.LifeCycle
import com.netflix.curator.framework.{CuratorFrameworkFactory, CuratorFramework}
import com.netflix.curator.retry.ExponentialBackoffRetry

/**
 * Zookeeper Service interface
 *
 * @author Taro L. Saito
 */
trait ZooKeeperService {

  import xerial.silk.cluster.config

  val zk : ZooKeeperClient

  def newZooKeeperConnection : ZooKeeperClient = {
    val cf = CuratorFrameworkFactory.newClient(config.zk.zkServersConnectString, config.zk.clientSessionTimeout, config.zk.clientConnectionTimeout, retryPolicy)
    val c = new ZooKeeperClient(cf)
    c
  }
  //def retryPolicy = new ExponentialBackoffRetry(config.zk.clientConnectionTimeout, config.zk.clientConnectionMaxRetry)
  def retryPolicy = new ExponentialBackoffRetry(1000, 2)
}


