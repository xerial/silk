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
import com.netflix.curator.framework.CuratorFramework
import xerial.silk.cluster.SilkClient.{Status, ClientInfo}
import com.netflix.curator.utils.EnsurePath
import xerial.silk.core.SilkSerializer
import java.util.concurrent.TimeoutException

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

  def listServerStatus = {
    ZooKeeper.withZkClient(ZooKeeper.defaultZKServerAddr) { zkCli =>
      listServerStatusWith(zkCli)
    }
  }


  def listServerStatusWith(zkCli:CuratorFramework) = {
    val children = zkCli.getChildren.forPath(config.zk.clusterNodePath)
    import collection.JavaConversions._
    children.par.flatMap { c =>
      getClientInfo(zkCli, c) map {ci =>
        import akka.pattern.ask
        import akka.dispatch.Await
        import akka.util.Timeout
        import akka.util.duration._

        val sc = SilkClient.getClientAt(ci.m.host.address)
        val status =
          try {
            implicit val timeout = Timeout(10 seconds)
            val reply = (sc ? Status).mapTo[String]
            Await.result(reply, timeout.duration)
          }
          catch {
            case e: TimeoutException => {
              warn("request for %s is timed out", ci.m.hostname)
              "No response"
            }
          }
        val m = ci.m
        Seq((ci, status))

      } getOrElse (Seq.empty)
    }
  }

  private[cluster] def getClientInfo(zkCli:CuratorFramework, hostName:String) : Option[ClientInfo] = {
    val nodePath = config.zk.clusterNodePath + "/%s".format(hostName)
    new EnsurePath(config.zk.clusterNodePath).ensure(zkCli.getZookeeperClient)
    val clusterEntry = zkCli.checkExists().forPath(nodePath)
    if(clusterEntry == null)
      None
    else {
      val data = zkCli.getData.forPath(nodePath)
      try {
        Some(SilkSerializer.deserializeAny(data).asInstanceOf[ClientInfo])
      }
      catch {
        case e =>
          warn(e)
          None
      }
    }
  }

  private[cluster] def setClientInfo(zkCli:CuratorFramework, hostName:String, ci:ClientInfo) {
    val nodePath = config.zk.clusterNodePath + "/%s".format(hostName)
    new EnsurePath(config.zk.clusterNodePath).ensure(zkCli.getZookeeperClient)
    val ciSer = SilkSerializer.serialize(ci)
    if(zkCli.checkExists.forPath(nodePath) == null)
      zkCli.create().forPath(nodePath, ciSer)

    zkCli.setData().forPath(nodePath, ciSer)
  }





}

