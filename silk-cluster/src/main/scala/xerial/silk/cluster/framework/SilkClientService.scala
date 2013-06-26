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
import akka.actor.{Actor, ActorRef}
import java.util.UUID
import xerial.core.io.IOUtil
import xerial.silk.framework.ops.Silk
import xerial.silk.core.ClosureSerializer


/**
 * @author Taro L. Saito
 */
trait SilkClientService
  extends SilkFramework
  with DistributedCache
  with ClusterNodeManager
  with ZooKeeperService
  with DefaultConsoleLogger
  with DistributedSliceStorage
  with ZookeeperConnectionFailureHandler
  with LocalTaskManagerComponent
  with DistributedTaskMonitor
  with LifeCycle
{

  val host: Host
  val zk: ZooKeeperClient
  val dataServer: DataServer
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

//    def submitEvalTask[A, B](op: Silk[A], sliceIndex: Int) {
//      val task = TaskRequest(UUID.randomUUID(), ClosureSerializer.serializeClosure({
//        val c = SilkClient.client.get
//        val inputSlice : Slice[_] = c.sliceStorage.get(op.in, sliceIndex).get
//        val sliceData = c.sliceStorage.retrieve(op.in, inputSlice)
//        val result = sliceData.map(m.fwrap).asInstanceOf[Seq[A]]
//        val slice = Slice(c.currentNodeName, sliceIndex)
//        c.sliceStorage.put(op, sliceIndex, slice)
//      }
//      ), Seq.empty)
//      sendToMaster(task)
//    }
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




