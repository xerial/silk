//--------------------------------------
//
// ZooKeeperService.scala
// Since: 2013/06/11 15:30
//
//--------------------------------------

package xerial.silk.cluster.framework

import xerial.silk.cluster._
import xerial.silk.framework.{LifeCycle, ConfigComponent}
import com.netflix.curator.framework.{CuratorFrameworkFactory, CuratorFramework}
import com.netflix.curator.retry.ExponentialBackoffRetry

/**
 * Zookeeper Service interface
 *
 * @author Taro L. Saito
 */
trait ZooKeeperService {

  import xerial.silk.cluster.config

  val zkClient : ZooKeeperClient

  def newZooKeeperConnection : ZooKeeperClient = {
    val cf = CuratorFrameworkFactory.newClient(config.zk.zkServersConnectString, config.zk.clientSessionTimeout, config.zk.clientConnectionTimeout, retryPolicy)
    val c = new ZooKeeperClient(cf)
    c
  }
  private def retryPolicy = new ExponentialBackoffRetry(config.zk.clientConnectionTickTime, config.zk.clientConnectionMaxRetry)
}


trait ZooKeeperStarter extends LifeCycle {
  self:ZooKeeperService =>

  lazy val zkClient : ZooKeeperClient = newZooKeeperConnection

  abstract override def startUp = {
    super.startUp
    zkClient.start
  }

  abstract override def tearDown = {
    zkClient.close
    super.tearDown
  }

}



