package xerial.silk

import java.io.File
import java.net.InetAddress
import xerial.core.log.Logger
import scala.util.DynamicVariable

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



  val localhost: Host = {
    val lh = InetAddress.getLocalHost
    Host(lh.getHostName, lh.getHostAddress)
  }



  // TODO setting configurations from SILK_CONFIG file
  val config = new DynamicVariable[xerial.silk.cluster.Config](Config())

  def withConfig[U](c:Config)(f: => U) : U = config.withValue[U](c)(f)

  //val config = new Config


}
