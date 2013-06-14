//--------------------------------------
//
// SilkClientService.scala
// Since: 2013/06/12 16:21
//
//--------------------------------------

package xerial.silk.cluster.framework

import xerial.silk.cluster._
import xerial.silk.framework._
import com.netflix.curator.framework.state.{ConnectionState, ConnectionStateListener}
import com.netflix.curator.framework.CuratorFramework


/**
 * @author Taro L. Saito
 */
trait SilkClientService
  extends SilkFramework
  with DistributedCache
  with ClusterNodeManager
  with ZooKeeperService
  with DefaultConsoleLogger
  with ZookeeperConnectionFailureHandler
  with LifeCycle
{

  val host: Host
  val zk: ZooKeeperClient
  val dataServer: DataServer
  val leaderSelector:SilkMasterSelector

//  type Session = SilkSession
//  def run[A](session: Session, op: Silk[A]) = {
//    // TODO impl
//
//  }


  abstract override def startup {
    trace("SilkClientService start up")
    super.startup
  }

  abstract override def teardown {
    trace("SilkClientService tear down")
    super.teardown
  }

}

trait ZookeeperConnectionFailureHandler extends ConnectionStateListener with LifeCycle {
  self: SilkClientService =>

  def onLostZooKeeperConnection : Unit

  /**
   * Called when there is a state change in the connection
   *
   * @param client the client
   * @param newState the new state
   */
  def stateChanged(client: CuratorFramework, newState: ConnectionState) {
    newState match {
      case ConnectionState.LOST =>
        warn("Connection to ZooKeeper is lost")
        onLostZooKeeperConnection
      case ConnectionState.SUSPENDED =>
        warn("Connection to ZooKeeper is suspended")
        onLostZooKeeperConnection
      case _ =>
    }
  }

  abstract override def startup {
    super.startup
    zk.curatorFramework.getConnectionStateListenable.addListener(self)
  }

  abstract override def teardown {
    super.teardown
    zk.curatorFramework.getConnectionStateListenable.removeListener(self)
  }
}


trait SilkMasterService
  extends SilkFramework
  with ClusterResourceManager
  with ZooKeeperService
  with DefaultConsoleLogger
{


}

