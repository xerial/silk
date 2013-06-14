//--------------------------------------
//
// SilkMasterSelector.scala
// Since: 2013/06/14 14:50
//
//--------------------------------------

package xerial.silk.cluster

import xerial.silk.framework.Host
import xerial.core.log.Logger
import akka.actor.{Props, ActorSystem}
import com.netflix.curator.framework.recipes.leader.{LeaderSelectorListener, LeaderSelector}
import com.netflix.curator.framework.CuratorFramework
import com.netflix.curator.framework.state.ConnectionState
import xerial.silk.cluster.framework.ActorService

/**
 * This class selects one of the silk clients as a SilkMaster.
 * @param zk
 * @param host
 */
private[cluster] class SilkMasterSelector(zk: ZooKeeperClient, host: Host) extends Logger {

  @volatile private var masterSystem: Option[ActorSystem] = None

  trace("Preparing SilkMaster selector")
  zk.ensurePath(config.zk.leaderElectionPath)
  private var leaderSelector: Option[LeaderSelector] = None


  def leaderID = leaderSelector.map {
    _.getLeader.getId
  } getOrElse ""

  private def shutdownMaster {
    synchronized {
      masterSystem map {
        info("Shut down the SilkMaster")
        _.shutdown
      }
      masterSystem = None
    }
  }


  def start {

    leaderSelector = Some(new LeaderSelector(zk.curatorFramework, config.zk.leaderElectionPath.path, new LeaderSelectorListener {
      def stateChanged(client: CuratorFramework, newState: ConnectionState) {
        if (newState == ConnectionState.LOST || newState == ConnectionState.SUSPENDED) {
          info(s"connection state is changed: $newState")
          shutdownMaster
        }
      }
      def takeLeadership(client: CuratorFramework) {
        info("Takes the leadership")
        if (isStopped) {
          info("But do not start SilkMaster since it is in termination phase")
          return
        }

        // Start up a master client
        masterSystem = Some(ActorService.getActorSystem(host.address, port = config.silkMasterPort))
        try {
          masterSystem map {
            sys =>
              sys.actorOf(Props(new SilkMaster(zk)), "SilkMaster")
              sys.awaitTermination()
          }
        }
        finally
          shutdownMaster
      }

    }))


    // Select a master among multiple clients
    // Start the leader selector
    val id = "%s:%s".format(host.address, config.silkMasterPort)
    leaderSelector.map(_.setId(id))
    //leaderSelector.autoRequeue
    leaderSelector.map(_.start())
    isStarted = true
  }

  private var isStarted = false
  private var isStopped = false

  def stop {
    if (isStarted && !isStopped) {
      synchronized {
        debug("Closing SilkMasterSelector")
        leaderSelector.map(_.close())
        isStopped = true
      }
    }

  }

}