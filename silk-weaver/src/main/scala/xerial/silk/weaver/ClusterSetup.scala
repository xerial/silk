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
import xerial.silk.io.ServiceGuard
import xerial.silk.cluster._
import SilkClient.SilkClientRef
import xerial.silk.cluster.store.{DataServerComponent, DataServer}
import xerial.silk.cluster.rm.ClusterNodeManager



/**
 * Launches SilkClient. This code must be in silk-weaver project since it depends on silk-webui project.
 * The project dependency order is silk-core <- silk-webui <- silk-weaver.
 *
 * @author Taro L. Saito
 */
object ClusterSetup extends Logger {


  def startClient[U](config:SilkClusterFramework#Config, host:Host, zkConnectString:String)(f:SilkClientService => U) : Unit = {
    val thisConfig = config
    val thisHost = host
    SilkCluster.setLocalHost(host)
    trace(s"Start SilkClient at $host")

    debug(s"zkConnectString: ${zkConnectString}")
    debug(s"cluster config: ${config.cluster}")
    debug(s"zk config: ${config.zk}")

    for{zkc <- ZooKeeper.zkClient(config.zk, zkConnectString) whenMissing
      { warn("No Zookeeper appears to be running. Run 'silk cluster start' first.")}} {

      val clusterManager = new ClusterNodeManager with ZooKeeperService with SilkClusterFramework {
        override lazy val config = thisConfig
        val zk : ZooKeeperClient = zkc
      }

      if(clusterManager.nodeManager.clientIsActive(host.name)) {
        // Avoid duplicate launch
        info("SilkClient is already running")
      }
      else {
        // Start a SilkClient

        for{
          system <- ActorService(host.address, port = config.cluster.silkClientPort)
          ds <- DataServer(config.home.silkTmpDir, config.cluster.dataServerPort, config.cluster.dataServerKeepAlive)
          leaderSel <- SilkMasterSelector(config, zkc, host)
          service = new SilkClientService {
            lazy val config = thisConfig
            val host = thisHost
            val zk = zkc
            val dataServer = ds
            def actorRef(addr:String) = { SilkException.NA }
          }
          webUI <- if(config.cluster.launchWebUI) SilkWebService(service) else ServiceGuard.empty
        } {
          val clientRef = new SilkClientRef(system, system.actorOf(Props(new SilkClient(config, host, zkc, leaderSel, ds)), "SilkClient"))
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
            f(service)
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