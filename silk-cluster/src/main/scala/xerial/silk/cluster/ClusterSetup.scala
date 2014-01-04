//--------------------------------------
//
// ClusterSetup.scala
// Since: 2013/07/18 10:31
//
//--------------------------------------

package xerial.silk.cluster

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


  def startClient[U](thisConfig:ClusterWeaverConfig, host:Host, zkConnectString:String)(f:SilkClientService => U) : Unit = {
    val thisHost = host
    SilkCluster.setLocalHost(host)
    trace(s"Start SilkClient at $host")

    debug(s"zkConnectString: ${zkConnectString}")
    debug(s"cluster config: ${thisConfig.cluster}")
    debug(s"zk config: ${thisConfig.zk}")

    for{zkc <- ZooKeeper.zkClient(thisConfig.zk, zkConnectString) whenMissing
      { warn("No Zookeeper appears to be running. Run 'silk cluster start' first.")}} {

      val clusterManager = new ClusterNodeManager with ZooKeeperService with ClusterWeaver {
        override val config = thisConfig
        val zk : ZooKeeperClient = zkc
      }

      if(clusterManager.nodeManager.clientIsActive(host.name)) {
        // Avoid duplicate launch
        info("SilkClient is already running")
      }
      else {
        // Start a SilkClient
        for{
          leaderSel <- SilkMasterSelector(thisConfig, zkc, host)
          system <- ActorService(host.address, port = thisConfig.cluster.silkClientPort)
          ds <- DataServer(thisConfig.home.silkTmpDir, thisConfig.cluster.dataServerPort, thisConfig.cluster.dataServerKeepAlive)
        }{
          val service = new SilkClientService {
            override val config = thisConfig
            val host = thisHost
            @transient val zk = zkc
            @transient val dataServer = ds
            @transient val actorSystem = system
            def actorRef(addr:String) = { actorSystem.actorFor(addr) }
          }
          for(webUI <- if(thisConfig.cluster.launchWebUI) SilkWebService(service) else ServiceGuard.empty) {
            val clientRef = new SilkClientRef(system, system.actorOf(Props(new SilkClient(thisConfig, host, zkc, leaderSel, ds)), "SilkClient"))
            try {
              // Wait until the client has started
              val maxRetry = 10
              var retry = 0
              var clientIsReady = false
              while(!clientIsReady && retry < maxRetry) {
                try {
                  import scala.concurrent.duration._
                  val result = clientRef.?(ReportStatus, 5.seconds)
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
              info("Self-termination phase")
              clientRef ! Terminate
            }
          }
        }
      }
    }
  }

}