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
trait ZooKeeperService extends LifeCycle {

  import xerial.silk.cluster.config

  lazy val zkClient : ZooKeeperClient = {
    val cf = CuratorFrameworkFactory.newClient(config.zk.zkServersConnectString, config.zk.clientSessionTimeout, config.zk.clientConnectionTimeout, retryPolicy)
    val c = new ZooKeeperClient(cf)
    c
  }

  private def retryPolicy = new ExponentialBackoffRetry(config.zk.clientConnectionTickTime, config.zk.clientConnectionMaxRetry)

  abstract override def start = {
    super.start
    zkClient.start
  }

  abstract override def terminate = {
    zkClient.close
    super.terminate
  }

}


