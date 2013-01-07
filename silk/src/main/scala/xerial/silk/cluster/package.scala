package xerial.silk

import java.io.File
import java.net.InetAddress
import xerial.core.log.Logger

/**
 * Cluster configuration parameters
 *
 * @author Taro L. Saito
 */
package object cluster extends Logger {

  import xerial.core.io.Path._

  val SILK_HOSTS = SILK_HOME / "hosts"
  val ZK_HOSTS = SILK_HOME / "zkhosts"
  val SILK_CONFIG = SILK_HOME / "config.silk"
  val SILK_LOCALDIR = SILK_HOME / "local"
  val SILK_TMPDIR = SILK_LOCALDIR / "tmp"
  val SILK_LOGDIR = SILK_LOCALDIR / "log"

  for(d <- Seq(SILK_LOCALDIR, SILK_TMPDIR, SILK_LOGDIR)) {
    if(!d.exists)
      d.mkdirs
  }


  val localhost: Host = {
    val lh = InetAddress.getLocalHost
    Host(lh.getHostName, lh.getHostAddress)
  }

  case class Config(silkClientPort: Int = 8980,
                    dataServerPort: Int = 8984,
                    zk: ZkConfig = ZkConfig())

  case class ZkConfig(basePath: String = "/xerial/silk",
                      clusterPathSuffix : String = "cluster",
                      statusPathSuffix: String = "zk/status",
                      quorumPort: Int = 8981,
                      leaderElectionPort: Int = 8982,
                      clientPort: Int = 8983,
                      tickTime: Int = 2000,
                      initLimit: Int = 10,
                      syncLimit: Int = 5,
                      dataDir: File = new File(SILK_LOCALDIR, "zk")) {
    val statusPath = basePath + "/" + statusPathSuffix
    val clusterPath = basePath + "/" + clusterPathSuffix
    val clusterNodePath = basePath + "/" + clusterPathSuffix + "/node"
  }


  // TODO setting configurations from SILK_CONFIG file
  //val config = new Config


}
