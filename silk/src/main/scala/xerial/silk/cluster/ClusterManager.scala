//--------------------------------------
//
// ClusterManager.scala
// Since: 2012/12/13 3:15 PM
//
//--------------------------------------

package xerial.silk.cluster

import java.io.File
import io.Source
import java.net.{UnknownHostException, InetAddress}
import xerial.core.log.Logger
import xerial.core.util.Shell
import xerial.lens.cui.command
import xerial.silk

/**
 * @author Taro L. Saito
 */
object ClusterManager extends Logger {


  def defaultHosts(clusterFile:File = SILK_HOSTS): Seq[Host] = {
    if (clusterFile.exists()) {
      def getHost(hostname: String): Option[Host] = {
        try {
          val addr = InetAddress.getByName(hostname)
          Some(Host(hostname, addr.getHostAddress))
        }
        catch {
          case e: UnknownHostException => {
            warn("unknown host: %s", hostname)
            None
          }
        }
      }
      val hosts = for {
        (line, i) <- Source.fromFile(clusterFile).getLines.zipWithIndex;
        val host = line.trim
        if !host.isEmpty && !host.startsWith("#")
        h <- getHost(host)
      } yield h
      hosts.toSeq
    }
    else {
      warn("$HOME/.silk/hosts is not found. Use localhost only")
      Seq(localhost)
    }
  }

  /**
   * Check wheather silk is installed
   * @param h
   */
  def isSilkInstalled(h:Host) : Boolean = {
    val ret = Shell.exec("ssh -n %s '$SHELL -l -c silk version'".format(h.name))
    ret == 0
  }




}

