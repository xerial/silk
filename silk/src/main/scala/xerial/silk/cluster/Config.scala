//--------------------------------------
//
// Config.scala
// Since: 2013/01/07 11:18 AM
//
//--------------------------------------

package xerial.silk.cluster

import java.io.File

/**
 * Cluster configuration
 * @author Taro L. Saito
 */
case class Config(silkClientPort: Int = 8980,
                  dataServerPort: Int = 8981,
                  zk: ZkConfig = ZkConfig()) {


  for(d <- Seq(SILK_LOCALDIR, SILK_TMPDIR, SILK_LOGDIR) if !d.exists) d.mkdirs

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
 * @param dataDir
 */
case class ZkConfig(basePath: String = "/xerial/silk",
                    clusterPathSuffix : String = "cluster",
                    statusPathSuffix: String = "zk/status",
                    quorumPort: Int = 8982,
                    leaderElectionPort: Int = 8983,
                    clientPort: Int = 8984,
                    tickTime: Int = 2000,
                    initLimit: Int = 10,
                    syncLimit: Int = 5,
                    dataDir: File = new File(SILK_LOCALDIR, "zk")) {
  val statusPath = basePath + "/" + statusPathSuffix
  val clusterPath = basePath + "/" + clusterPathSuffix
  val clusterNodePath = basePath + "/" + clusterPathSuffix + "/node"

  for(d <- Seq(dataDir) if !d.exists) d.mkdirs

  def serverDir(id:Int) : File = new File(dataDir, "server.%d".format(id))
  def myIDFile(id:Int) : File = new File(serverDir(id), "myid")
}

object ZkConfig {
  val default = ZkConfig()
}
