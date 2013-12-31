//--------------------------------------
//
// SilkClusterFramework.scala
// Since: 2013/11/19 10:37
//
//--------------------------------------

package xerial.silk.cluster

import java.io.File
import xerial.silk.framework._
import xerial.silk.util.Path._
import xerial.core.log.{LoggerFactory, Logger}
import com.netflix.curator.retry.ExponentialBackoffRetry
import xerial.core.io.IOUtil
import xerial.silk.Weaver

object ClusterWeaver {

  def default = new ClusterWeaver {
    override val config = defaultConfig
  }
  def forTest(customZkConnectString:String) = {
    new ClusterWeaver {
      override lazy val zkConnectString = customZkConnectString
      override val config = {
        val tmpDir : File = IOUtil.createTempDir(new File("target"), "silk-tmp").getAbsoluteFile
        ClusterWeaverConfig(
          home = HomeConfig(tmpDir),
          cluster = ClusterConfig(
            silkClientPort = IOUtil.randomPort,
            dataServerPort = IOUtil.randomPort,
            webUIPort = IOUtil.randomPort,
            launchWebUI = false,
            silkMasterPort = IOUtil.randomPort
          ),
          zk = ZkConfig(zkHosts = tmpDir / "zkhosts", zkDir = tmpDir / "local" / "zk")
        )
      }
    }
  }


  def defaultConfig : ClusterWeaver#Config = ClusterWeaverConfig()

  def defaultClusterClient = new ClusterWeaver {
    override val config = defaultClusterClientConfig
  }

  def defaultClusterClientConfig : ClusterWeaver#Config = ClusterWeaverConfig(
    cluster = ClusterConfig(dataServerKeepAlive = false) // To quickly close DataServer
  )


}

case class ClusterWeaverConfig(cluster:ClusterConfig = ClusterConfig(), home:HomeConfig=HomeConfig(), zk:ZkConfig=ZkConfig())


/**
 * A framework for evaluating Silk operations in a cluster machine
 * @author Taro L. Saito
 */
trait ClusterWeaver
  extends Weaver
{
  type Config = ClusterWeaverConfig
  val config : Config = ClusterWeaverConfig()

  lazy val zkServers = {
    val logger = LoggerFactory(classOf[ClusterWeaver])
    // read zkServer lists from $HOME/.silk/zkhosts file
    val ensembleServers: Seq[ZkEnsembleHost] = ZooKeeper.readHostsFile(config.zk, config.zk.zkHosts) getOrElse {
      logger.debug(s"Selecting candidates of zookeeper servers from hosts files")
      val candidates = Host.readHostsFile(config.home.silkHosts)
      val selectedHosts = if(candidates.length >= 3)
        Seq() ++ candidates.take(3) // use first three hosts as zk servers
      else if(candidates.length > 1){
        logger.warn(s"Not enough servers found in ${config.home.silkHosts} file (required more than 3 servers for the reliability). Start with a single zookeeper server")
        candidates.take(1)
      }
      else {
        logger.warn("Use localhost as a single zookeeper server")
        Seq(SilkCluster.localhost)
      }
      selectedHosts.map(h => new ZkEnsembleHost(h, config.zk.quorumPort, config.zk.leaderElectionPort, config.zk.clientPort))
    }

    logger.debug(s"Selected zookeeper servers: ${ensembleServers.mkString(",")}")
    ensembleServers
  }

  lazy val zkConnectString = zkServers.map(_.connectAddress).mkString(",")
  lazy val zkServerString = zkServers.map(_.serverAddress).mkString(" ")

  def defaultZkClient = ZooKeeper.zkClient(config.zk, zkConnectString)
  def logFile(hostName: String): File = new File(config.home.silkLogDir, s"${hostName}.log")

  def awaitTermination {}

  //def hosts : Seq[Host] = Seq.empty


}




case class ClusterConfig(silkMasterPort: Int = 8983,
                         silkClientPort: Int = 8984,
                         dataServerPort: Int = 8985,
                         dataServerKeepAlive: Boolean = true,
                         webUIPort : Int = 8986,
                         launchWebUI : Boolean = true,
                         resourceMonitoringIntervalSec : Int = 60
)

trait ClusterConfigComponent {

  val cluster = new ClusterConfig

}

object ZkConfig {

  def defaultZkClientPort = 8980


}


case class ZkConfig(zkHosts : File = HomeConfig.defaultSilkHome / "zkhosts",
                    zkDir : File = HomeConfig.defaultSilkHome / "local" / "zk",
                    basePath: ZkPath = ZkPath("/silk"),
                    clientPort: Int = ZkConfig.defaultZkClientPort,
                    quorumPort: Int = 8981,
                    leaderElectionPort: Int = 8982,
                    tickTime: Int = 2000,
                    initLimit: Int = 10,
                    syncLimit: Int = 5,
                    maxClientConnection : Int = 128,
                    clientConnectionMaxRetry : Int = 6,
                    clientConnectionTickTime : Int = 1000,
                    clientSessionTimeout : Int = 60 * 1000,
                    clientConnectionTimeout : Int = 15 * 1000)
  extends Logger {

  val statusPath = basePath / "zkstatus"
  val cachePath = basePath / "cache"
  val clusterPath = basePath / "cluster"
  val clusterStatePath = clusterPath / "global-status"
  val clusterNodePath = clusterPath / "node"
  val clusterNodeStatusPath = clusterPath / "node-status"
  val leaderElectionPath = clusterPath / "le"
  val masterInfoPath = clusterPath / "master"

  def zkServerDir(id:Int) : File = new File(zkDir, "server.%d".format(id))
  def zkMyIDFile(id:Int) : File = new File(zkServerDir(id), "myid")

  def clientEntryPath(hostName:String) : ZkPath = clusterNodePath / hostName

  def retryPolicy = {
    new ExponentialBackoffRetry(clientConnectionTickTime, clientConnectionMaxRetry)
  }


}


trait ZooKeeperConfigComponent {

  val zk : ZkConfig = new ZkConfig

}


