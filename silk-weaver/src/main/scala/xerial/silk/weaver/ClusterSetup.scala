//--------------------------------------
//
// ClusterSetup.scala
// Since: 2013/07/18 10:31
//
//--------------------------------------

package xerial.silk.weaver

import xerial.core.log.Logger
import xerial.silk.framework.Host
import xerial.silk.cluster._
import SilkClient._
import xerial.silk.cluster.framework.{ActorService, ZooKeeperService, ClusterNodeManager}
import akka.actor.Props
import java.util.concurrent.TimeoutException
import xerial.silk.cluster.SilkClient.SilkClientRef
import xerial.silk.cluster.SilkClient.ClientEnv
import xerial.silk.webui.SilkWebService

/**
 * @author Taro L. Saito
 */
object ClusterSetup extends Logger {


  def startClient[U](host:Host, zkConnectString:String)(f:ClientEnv => U) : Unit = {
    for (zkc <- ZooKeeper.zkClient(zkConnectString) whenMissing {
      warn("No Zookeeper appears to be running. Run 'silk cluster start' first.")
    }) {
      startClient(host, zkc)(f)
    }
  }

  def startClient[U](host:Host, zkc:ZooKeeperClient)(f:ClientEnv => U) : Unit = {

    setLocalHost(host)

    trace(s"Start SilkClient at $host")

    val clusterManager = new ClusterNodeManager with ZooKeeperService {
      val zk : ZooKeeperClient = zkc
    }

    if(clusterManager.clientIsActive(host.name)) {
      // Avoid duplicate launch
      info("SilkClient is already running")
    }
    else {
      // Start a SilkClient
      for{
        system <- ActorService(host.address, port = config.silkClientPort)
        dataServer <- DataServer(config.dataServerPort)
        webUI <- SilkWebService(config.webUIPort)
        leaderSelector <- SilkMasterSelector(zkc, host)
      } {
        val env = ClientEnv(new SilkClientRef(system, system.actorOf(Props(new SilkClient(host, zkc, leaderSelector, dataServer)), "SilkClient")), zkc)
        try {
          // Wait until the client has started
          val maxRetry = 10
          var retry = 0
          var clientIsReady = false
          while(!clientIsReady && retry < maxRetry) {
            try {
              val result = env.clientRef ? (ReportStatus)
              result match {
                case OK => clientIsReady = true
              }
            }
            catch {
              case e: TimeoutException =>
                retry += 1
            }
          }
          trace("SilkClient is ready")
          // exec user code
          f(env)
        }
        catch {
          case e:Exception => error(e)
        }
        finally {
          trace("Self-termination phase")
          env.clientRef ! Terminate
        }
      }
    }
  }

}