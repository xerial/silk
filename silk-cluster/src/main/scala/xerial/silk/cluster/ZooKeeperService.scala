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
trait ZooKeeperService extends SilkClusterFramework {

  val zk : ZooKeeperClient

  object zookeeperService {

    def newZooKeeperConnection : ZooKeeperClient = {
      val cf = CuratorFrameworkFactory.newClient(zkServersConnectString, config.zk.clientSessionTimeout, config.zk.clientConnectionTimeout, retryPolicy)
      val c = new ZooKeeperClient(cf)
      c
    }


    /**
     * Get a ZooKeeper client. It will retry connection to the server the number of times specified by config.zk.clientConnectionMaxRetry.
     *
     * @return connection wrapper that can be used in for-comprehension
     */
    def defaultZkClient : ServiceGuard[ZooKeeperClient]  = zkClient(zkServersConnectString)

    def isAvailable : Boolean = ZooKeeper.isAvailable(getZkServers)

    private def retryPolicy = {
      new ExponentialBackoffRetry(config.zk.clientConnectionTickTime, config.zk.clientConnectionMaxRetry)
    }

    def zkClient(zkConnectString:String) : ServiceGuard[ZooKeeperClient] = {
      try {
        val cf = CuratorFrameworkFactory.newClient(zkConnectString, config.zk.clientSessionTimeout, config.zk.clientConnectionTimeout, retryPolicy)
        val c = new ZooKeeperClient(cf)
        c.start
        new ServiceGuard[ZooKeeperClient] {
          protected[silk] val service = c
          def close { c.close }
        }
      }
      catch {
        case e : Exception =>
          error(e)
          new MissingService[ZooKeeperClient]()
      }
    }

    /**
     * Get the default zookeeper servers
     * @return
     */
    lazy val defaultZKServers: Seq[ZkEnsembleHost] = {

      // read zkServer lists from $HOME/.silk/zkhosts file
      val ensembleServers: Seq[ZkEnsembleHost] = ZooKeeper.readHostsFile(config.zk.zkHosts) getOrElse {
        debug(s"Selecting candidates of zookeeper servers from ${config.home.silkHosts}")
        val zkHosts = for(candidates <- ZooKeeper.readHostsFile(config.home.silkHosts) if candidates.length > 0) yield {
          if(candidates.length >= 3)
            Seq() ++ candidates.take(3) // use first three hosts as zk servers
          else {
            warn(s"Not enough servers found in ${config.home.silkHosts} file (required more than 3 servers for the reliability). Start with a single zookeeper server")
            candidates.take(1)
          }
        }

        zkHosts.getOrElse {
          warn("Use localhost as a single zookeeper server")
          Seq(new ZkEnsembleHost(SilkCluster.localhost))
        }
      }

      debug(s"Selected zookeeper servers: ${ensembleServers.mkString(",")}")
      ensembleServers
    }

    def getZkServers = config.zk.zkServers getOrElse defaultZKServers
    def zkServersConnectString = getZkServers.map(_.connectAddress).mkString(",")
  }

}


