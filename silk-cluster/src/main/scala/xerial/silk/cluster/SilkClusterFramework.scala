//--------------------------------------
//
// SilkClusterFramework.scala
// Since: 2013/11/19 10:37
//
//--------------------------------------

package xerial.silk.cluster

import java.io.File
import xerial.silk.framework.{HomeConfigComponent, HomeConfig, SilkFramework}
import xerial.silk.util.Path
import Path._
import xerial.core.log.Logger
import xerial.core.io.IOUtil

/**
 * A framework for evaluating Silk operations in a cluster machine
 * @author Taro L. Saito
 */
trait SilkClusterFramework extends SilkFramework with Logger {

  type Config = ClusterConfigComponent
    with HomeConfigComponent
    with ZooKeeperConfigComponent

  object config extends ClusterConfigComponent with ZooKeeperConfigComponent with HomeConfigComponent

  private[silk] def testConfig(zkConnectString:String) : Config = {
    debug(s"Create a config for testing: zkConnectString = $zkConnectString")
    val tmpDir : File = IOUtil.createTempDir(new File("target"), "silk-tmp").getAbsoluteFile
    val c = zkConnectString.split(",")
    val zkHosts = c.map(ZkEnsembleHost(_)).toSeq
    new ClusterConfigComponent with ZooKeeperConfigComponent with HomeConfigComponent {
      override lazy val home = HomeConfig()
      override lazy val cluster = ClusterConfig(
        silkClientPort = IOUtil.randomPort,
        dataServerPort = IOUtil.randomPort,
        webUIPort = IOUtil.randomPort,
        launchWebUI = false,
        silkMasterPort = IOUtil.randomPort
      )
      override lazy val zk = ZkConfig(zkServers = Some(zkHosts))
    }
  }

}

case class ClusterConfig(silkMasterPort: Int = 8983,
                         silkClientPort: Int = 8984,
                         dataServerPort: Int = 8985,
                         dataServerKeepAlive: Boolean = true,
                         webUIPort : Int = 8986,
                         launchWebUI : Boolean = true)

trait ClusterConfigComponent {

  val cluster = new ClusterConfig

}

object ZkConfig {


}


case class ZkConfig(basePath: ZkPath = ZkPath("/silk"),
                    clientPort: Int = 8980,
                    quorumPort: Int = 8981,
                    leaderElectionPort: Int = 8982,
                    tickTime: Int = 2000,
                    initLimit: Int = 10,
                    syncLimit: Int = 5,
                    maxClientConnection : Int = 128,
                    clientConnectionMaxRetry : Int = 6,
                    clientConnectionTickTime : Int = 1000,
                    clientSessionTimeout : Int = 60 * 1000,
                    clientConnectionTimeout : Int = 15 * 1000,
                    private[silk] val zkServers : Option[Seq[ZkEnsembleHost]] = None) extends Logger {
  val statusPath = basePath / "zkstatus"
  val cachePath = basePath / "cache"
  val clusterPath = basePath / "cluster"
  val clusterStatePath = clusterPath / "global-status"
  val clusterNodePath = clusterPath / "node"
  val leaderElectionPath = clusterPath / "le"
  val masterInfoPath = clusterPath / "master"


  def clientEntryPath(hostName:String) : ZkPath = clusterNodePath / hostName

}


trait ZooKeeperConfigComponent {

  lazy val zk = new ZkConfig

}
