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

  val SILK_HOSTS: File = new File(SILK_HOME, "hosts")
  val ZK_HOSTS: File = new File(SILK_HOME, "zkhosts")
  val SILK_CONFIG: File = new File(SILK_HOME, "config.silk")


  val localhost: Host = {
    val lh = InetAddress.getLocalHost
    trace("localhost:%s, %s", lh.getHostName, lh.getHostAddress)
    Host(lh.getHostName, lh.getHostAddress)
  }

  case class Config(silkClientPort: Int = 8980,
                    zk: ZkConfig = ZkConfig())

  case class ZkConfig(basePath: String = "/xerial/silk",
                      statusPathSuffix: String = "/zk/status",
                      quorumPort: Int = 8981,
                      leaderElectionPort: Int = 8982,
                      clientPort: Int = 8983,
                      tickTime: Int = 2000,
                      initLimit: Int = 10,
                      syncLimit: Int = 5,
                      dataDir: File = new File(SILK_HOME, "zk")) {
    def statusPath = basePath + statusPathSuffix
  }


  // TODO setting configurations from SILK_CONFIG file
  val config = new Config


}
