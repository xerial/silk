//--------------------------------------
//
// SilkClientService.scala
// Since: 2013/06/12 16:21
//
//--------------------------------------

package xerial.silk.cluster

import com.netflix.curator.framework.state.{ConnectionState, ConnectionStateListener}
import com.netflix.curator.framework.CuratorFramework
import akka.actor.{Actor, ActorRef}
import java.util.UUID
import xerial.core.log.Logger
import xerial.silk.SilkException
import xerial.silk.cluster.store.{DataServerComponent, DistributedSliceStorage, DistributedCache}
import xerial.silk.framework._
import xerial.silk.cluster.rm.ClusterNodeManager
import xerial.silk.framework.scheduler.{TaskStatusUpdate, TaskStatus}


/**
 * Client service deployed at each host
 * @author Taro L. Saito
 */
trait SilkClientService
  extends SilkClusterFramework
  with DistributedCache
  with ClusterNodeManager
  with ZooKeeperService
  with DistributedSliceStorage
  with DataServerComponent
  with ZookeeperConnectionFailureHandler
  with LocalTaskManagerComponent
  with LocalClientComponent
  with DistributedTaskMonitor
  with DefaultExecutor
  with ClassBoxComponent
  with LifeCycle
  with LocalClient
  with MasterRecordComponent
  with Logger
{

  val host: Host
  val zk: ZooKeeperClient
  val leaderSelector:SilkMasterSelector
  def master : ActorRef

  def currentNodeName = host.name

  val localTaskManager = new LocalTaskManager {
    protected def sendToMaster(task:TaskRequest) {
      master ! task
    }
    protected def sendToMaster(taskID: UUID, status: TaskStatus) {
      master ! TaskStatusUpdate(taskID, status)
    }

    def getClassBox(classBoxID: UUID) = {
      SilkException.NA
    }
  }


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




