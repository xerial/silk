//--------------------------------------
//
// Config.scala
// Since: 2013/01/07 11:18 AM
//
//--------------------------------------

package xerial.silk.cluster

import java.io.File
import xerial.core.io.Path._
import xerial.core.log.Logger
import ZooKeeper._

object Config extends Logger {
  private[cluster] def defaultSilkHome : File = {
    sys.props.get("silk.home") map { new File(_) } getOrElse {
      val homeDir = sys.props.get("user.home") getOrElse ("")
      new File(homeDir, ".silk")
    }
  }

  /**
   * Get the default zookeeper servers
   * @return
   */
  private[cluster] def defaultZKServers: Seq[ZkEnsembleHost] = {

    // read zkServer lists from $HOME/.silk/zkhosts file
    val ensembleServers: Seq[ZkEnsembleHost] = readHostsFile(config.zkHosts) getOrElse {
      info("Selecting candidates of zookeeper servers from %s", config.silkHosts)
      val randomHosts = readHostsFile(config.silkHosts) filter {
        hosts => hosts.length >= 3
      } map {
        hosts =>
          Seq() ++ hosts.take(3) // use first three hosts as zk servers
      }
      randomHosts.getOrElse {
        warn("Not enough servers found in %s file (required more than 3 servers). Using localhost as a single zookeeper master", config.silkHosts)
        Seq(new ZkEnsembleHost(localhost.name))
      }
    }

    debug("Selected zookeeper servers: %s", ensembleServers.mkString(","))
    ensembleServers
  }

}


/**
 * Cluster configuration
 * @author Taro L. Saito
 */
case class Config(silkHome : File = Config.defaultSilkHome,
                  silkMasterPort: Int = 8980,
                  silkClientPort: Int = 8981,
                  dataServerPort: Int = 8982,
                  zk: ZkConfig = ZkConfig()) {
  val silkHosts : File = silkHome / "hosts"
  val zkHosts : File = silkHome / "zkhosts"
  val silkConfig : File = silkHome / "config.silk"
  val silkLocalDir : File = silkHome / "local"
  val silkTmpDir : File = silkLocalDir / "tmp"
  val silkLogDir : File = silkLocalDir / "log"
  val zkDir : File = silkLocalDir / "zk"

  for(d <- Seq(silkLocalDir, silkTmpDir, silkLogDir, zkDir) if !d.exists) d.mkdirs

  def zkServerDir(id:Int) : File = new File(zkDir, "server.%d".format(id))
  def zkMyIDFile(id:Int) : File = new File(zkServerDir(id), "myid")
}

/**
 * Zookeeper configuration
 * @param basePath
 * @param clusterPathSuffix
 * @param statusPathSuffix
 * @param quorumPort
 * @param leaderElectionPort
 * @param clientPort
 * @param tickTime
 * @param initLimit
 * @param syncLimit
 * @param zkServers comma separated string of (zookeeper address):(quorumPort):(leaderElectionPort)
 */
case class ZkConfig(basePath: String = "/silk",
                    clusterPathSuffix : String = "cluster",
                    statusPathSuffix: String = "zk/status",
                    quorumPort: Int = 8983,
                    leaderElectionPort: Int = 8984,
                    clientPort: Int = 8985,
                    tickTime: Int = 2000,
                    initLimit: Int = 10,
                    syncLimit: Int = 5,
                    private val zkServers : Option[Seq[ZkEnsembleHost]] = None) {
  val statusPath = basePath + "/" + statusPathSuffix
  val clusterPath = basePath + "/" + clusterPathSuffix
  val clusterNodePath = clusterPath + "/node"
  val leaderElectionPath = clusterPath + "/leaderelection"

  def getZkServers = zkServers getOrElse Config.defaultZKServers

  def zkServersString = getZkServers.map(_.clientAddress).mkString(",")
}


