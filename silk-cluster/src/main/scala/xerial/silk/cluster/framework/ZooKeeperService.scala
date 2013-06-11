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
 * Service interface for using ZooKeeper
 *
 * @author Taro L. Saito
 */
trait ZooKeeperService extends ConfigComponent with LifeCycle {

  type Config <: ZooKeeperConfig

  lazy val zkClient : ZooKeeperClient = {
    val cf = CuratorFrameworkFactory.newClient(config.zkConnectString, config.clientSessionTimeout, config.clientConnectionTimeout, retryPolicy)
    val c = new ZooKeeperClient(cf)
    c
  }

  private def retryPolicy = new ExponentialBackoffRetry(config.clientConnectionTickTime, config.clientConnectionMaxRetry)

  abstract override def start = {
    super.start
    zkClient.start
  }

  abstract override def terminate = {
    zkClient.close
    super.terminate
  }

  trait ZooKeeperConfig {
    def zkConnectString : String
    def basePath: ZkPath = ZkPath("/silk")
    def zkClientPort: Int = 8980
    def tickTime: Int = 2000
    def initLimit: Int = 10
    def syncLimit: Int = 5
    def clientConnectionMaxRetry : Int = 5
    def clientConnectionTickTime : Int = 500
    def clientSessionTimeout : Int = 60 * 1000
    def clientConnectionTimeout : Int = 3  * 1000
  }

}


