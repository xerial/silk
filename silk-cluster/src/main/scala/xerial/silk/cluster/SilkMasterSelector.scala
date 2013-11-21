//--------------------------------------
//
// SilkMasterSelector.scala
// Since: 2013/06/14 14:50
//
//--------------------------------------

package xerial.silk.cluster

import xerial.core.log.Logger
import akka.actor.{Props, ActorSystem}
import com.netflix.curator.framework.recipes.leader.{LeaderSelectorListener, LeaderSelector}
import com.netflix.curator.framework.CuratorFramework
import com.netflix.curator.framework.state.ConnectionState
import xerial.silk.io.ServiceGuard
import xerial.silk.framework.Host

object SilkMasterSelector {

  def apply(config:SilkClusterFramework#Config, zk:ZooKeeperClient, host:Host) = new ServiceGuard[SilkMasterSelector] {
    protected[silk] val service = new SilkMasterSelector(config, zk, host)
    service.start
    def close {
      service.stop
    }
  }

}


/**
 * This class selects one of the SilkClients as a SilkMaster.
 * @param zk
 * @param host
 */
private[silk] class SilkMasterSelector(config:SilkClusterFramework#Config, zk: ZooKeeperClient, host: Host) extends Logger {

  @volatile private var masterSystem: Option[ActorSystem] = None

  debug(s"Preparing SilkMaster selector: ${config.zk.leaderElectionPath}")
  zk.ensurePath(config.zk.leaderElectionPath)
  private var leaderSelector: Option[LeaderSelector] = None


  def leaderID = leaderSelector.map {
    _.getLeader.getId
  } getOrElse ""

  private def shutdownMaster {
    synchronized {
      masterSystem map {
        trace("Shut down the SilkMaster")
        _.shutdown
      }
      masterSystem = None
    }
  }


  def start {
    leaderSelector = Some(new LeaderSelector(zk.curatorFramework, config.zk.leaderElectionPath.path, new LeaderSelectorListener {
      def stateChanged(client: CuratorFramework, newState: ConnectionState) {
        if (newState == ConnectionState.LOST || newState == ConnectionState.SUSPENDED) {
          warn(s"connection state is changed: $newState")
          shutdownMaster
        }
      }
      def takeLeadership(client: CuratorFramework) {

        val globalStatus = zk.get(config.zk.clusterStatePath).map(new String(_)).getOrElse("")
        if(globalStatus == "shutdown") {
          info("Takes the leadership, but do not start SilkMaster since the cluster is in the shutdown phase")
          return
        }
        info("Takes the leadership")
        if (isStopped)
          return

        // Start up a master client
        masterSystem = Some(ActorService.getActorSystem(host.address, port = config.cluster.silkMasterPort))
        try {
          masterSystem map {
            sys =>
              sys.actorOf(Props(new SilkMaster(config, host.name, host.address, zk)), "SilkMaaster")
              sys.awaitTermination()
          }
        }
        finally
          shutdownMaster
      }

    }))


    // Select a master among multiple clients
    // Start the leader selector
    val id = "%s:%s".format(host.address, config.cluster.silkMasterPort)
    leaderSelector.map(_.setId(id))
    //leaderSelector.autoRequeue
    leaderSelector.map(_.start())
    debug("Start SilkMasterSelector")
    isStarted = true
  }

  private var isStarted = false
  private var isStopped = false

  def stop {
    if (isStarted && !isStopped) {
      synchronized {
        trace("Stopped SilkMasterSelector")
        leaderSelector.map(_.close())
        isStopped = true
      }
    }

  }

}