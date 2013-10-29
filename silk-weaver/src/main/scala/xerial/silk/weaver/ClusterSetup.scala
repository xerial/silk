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
import SilkClient._
import akka.actor.Props
import java.util.concurrent.TimeoutException
import SilkClient.SilkClientRef
import SilkClient.ClientEnv
import xerial.silk.webui.SilkWebService
import xerial.silk.Silk

/**
 * Launches SilkClient. This code must be in silk-weaver project since it depends on silk-webui project.
 * The project dependency order is silk-core <- silk-webui <- silk-weaver.
 *
 * @author Taro L. Saito
 */
object ClusterSetup extends Logger {


  def startClient[U](host:Host, zkConnectString:String)(f:ClientEnv => U) : Unit = {
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
          webUI <- SilkWebService(config.webUIPort)
          leaderSelector <- SilkMasterSelector(zkc, host)
        }
        {
          val silkEnv = new SilkEnvImpl(zkc, system, dataServer)
          Silk.setEnv(silkEnv)

          val env = ClientEnv(new SilkClientRef(system, system.actorOf(Props(new SilkClient(host, zkc, leaderSelector, dataServer)), "SilkClient")), zkc, system)
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

}