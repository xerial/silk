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


  val localhost: Host = {
    val lh = InetAddress.getLocalHost
    Host(lh.getHostName, lh.getHostAddress)
  }


  // TODO setting configurations from SILK_CONFIG file
  val config = new DynamicVariable[xerial.silk.cluster.Config](Config())

  def withConfig[U](c:Config)(f: => U) : U = config.withValue[U](c)(f)



}
