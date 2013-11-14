//--------------------------------------
//
// ClusterSetup.scala
// Since: 2013/07/18 10:31
//
//--------------------------------------

package xerial.silk.weaver

import xerial.core.log.Logger
import xerial.silk.framework._
import xerial.silk._
import xerial.silk.cluster._
import SilkClient._
import akka.actor.Props
import java.util.concurrent.TimeoutException
import xerial.silk.webui.SilkWebService
import xerial.silk.Silk
import xerial.silk.io.ServiceGuard
import xerial.silk.cluster._
import SilkClient.SilkClientRef
import xerial.silk.cluster.store.DataServer
import xerial.silk.cluster.rm.ClusterNodeManager

/**
 * Launches SilkClient. This code must be in silk-weaver project since it depends on silk-webui project.
 * The project dependency order is silk-core <- silk-webui <- silk-weaver.
 *
 * @author Taro L. Saito
 */
object ClusterSetup extends Logger {


  def startClient[U](host:Host, zkConnectString:String)(f:SilkEnv => U) : Unit = {
    Silk.setLocalHost(host)
    trace(s"Start SilkClient at $host")


    for{zkc <- ZooKeeper.zkClient(zkConnectString) whenMissing
      { warn("No Zookeeper appears to be running. Run 'silk cluster start' first.")}} {

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
          dataServer <- DataServer(config.dataServerPort, config.dataServerKeepAlive)
          webUI <- if(config.launchWebUI) SilkWebService(config.webUIPort) else ServiceGuard.empty
          leaderSelector <- SilkMasterSelector(zkc, host)
        }
        {
          val silkEnv = new SilkEnvImpl(zkc, system, dataServer)
          Silk.setEnv(silkEnv)
          val clientRef = new SilkClientRef(system, system.actorOf(Props(new SilkClient(host, zkc, leaderSelector, dataServer)), "SilkClient"))
          //val env = ClientEnv(new SilkClientRef(system, system.actorOf(Props(new SilkClient(host, zkc, leaderSelector, dataServer)), "SilkClient")), zkc, system)
          try {
            // Wait until the client has started
            val maxRetry = 10
            var retry = 0
            var clientIsReady = false
            while(!clientIsReady && retry < maxRetry) {
              try {
                val result = clientRef ? (ReportStatus)
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
            f(silkEnv)
          }
          catch {
            case e:Exception => error(e)
          }
          finally {
            trace("Self-termination phase")
            clientRef ! Terminate
          }
        }
      }
    }
  }

}