//--------------------------------------
//
// SilkClusterFramework.scala
// Since: 2013/11/19 10:37
//
//--------------------------------------

package xerial.silk.cluster

import java.io.File
import xerial.silk.framework.{BaseConfig, SilkFramework}
import xerial.silk.util.Path
import Path._

/**
 * A framework for evaluating Silk operations in a cluster machine
 * @author Taro L. Saito
 */
trait SilkClusterFramework extends SilkFramework {

  type Config = ClusterConfig
    with BaseConfig
    with ZooKeeperConfig

  object config extends ClusterConfig with ZooKeeperConfig with BaseConfig
}


trait ClusterConfig {

  val cluster : ClusterConfig = new ClusterConfig

  case class ClusterConfig(silkMasterPort: Int = 8983,
                           silkClientPort: Int = 8984,
                           dataServerPort: Int = 8985,
                           dataServerKeepAlive: Boolean = true,
                           webUIPort : Int = 8986,
                           launchWebUI : Boolean = true)

}


trait ZooKeeperConfig {
  self: BaseConfig =>

  val zk = new ZkConfig

  case class ZkConfig(zkHosts : File = base.silkHome / "zkhosts", basePath: ZkPath = ZkPath("/silk"),
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
                      private val zkServers : Option[Seq[ZkEnsembleHost]] = None) {
    val statusPath = basePath / "zkstatus"
    val cachePath = basePath / "cache"
    val clusterPath = basePath / "cluster"
    val clusterStatePath = clusterPath / "global-status"
    val clusterNodePath = clusterPath / "node"
    val leaderElectionPath = clusterPath / "le"
    val masterInfoPath = clusterPath / "master"
    val zkDir : File = base.silkLocalDir / "zk"

    def clientEntryPath(hostName:String) : ZkPath = clusterNodePath / hostName

    def getZkServers = zkServers getOrElse Config.defaultZKServers

    def zkServersConnectString = getZkServers.map(_.connectAddress).mkString(",")

    def zkServerDir(id:Int) : File = new File(zkDir, "server.%d".format(id))
    def zkMyIDFile(id:Int) : File = new File(zkServerDir(id), "myid")


  }


}
