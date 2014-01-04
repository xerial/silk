/*
 * Copyright 2012 Taro L. Saito
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//--------------------------------------
//
// SilkClient.scala
// Since: 2012/12/13 4:50 PM
//
//--------------------------------------

package xerial.silk.cluster

import akka.actor._
import xerial.core.log.{LoggerFactory, Logger}
import akka.pattern.ask
import scala.concurrent.Await
import akka.util.Timeout
import scala.concurrent.duration._
import xerial.core.util.Shell
import java.util.UUID
import xerial.silk.io.ServiceGuard
import xerial.silk.cluster.store.{DataServerComponent, DistributedSliceStorage, DistributedCache, DataServer}
import xerial.silk.framework._
import xerial.silk.framework.NodeResource
import xerial.silk.framework.Node
import xerial.silk._
import xerial.silk.framework.scheduler.{TaskStatus, TaskStatusUpdate}
import xerial.silk.cluster.rm.{ResourceMonitorComponent, ClusterNodeManager}
import com.netflix.curator.framework.CuratorFramework
import com.netflix.curator.framework.state.{ConnectionStateListener, ConnectionState}
import scala.Some
import xerial.silk.framework.scheduler.TaskStatusUpdate
import xerial.silk.framework.Node
import xerial.silk.cluster.SilkClient.SilkClientRef
import xerial.silk.cluster.SilkClient.Run
import xerial.silk.framework.NodeResource
import java.util.concurrent.TimeoutException
import scala.Some
import xerial.silk.framework.scheduler.TaskStatusUpdate
import xerial.silk.framework.Node
import xerial.silk.cluster.SilkClient.SilkClientRef
import xerial.silk.cluster.SilkClient.Run
import xerial.silk.framework.NodeResource


/**
 * SilkClient is a network interface that accepts command from the other hosts
 */
object SilkClient extends Logger {


  private[silk] var client: Option[SilkClient] = None
  val dataTable = collection.mutable.Map[String, AnyRef]()


  //case class ClientEnv(clientRef: SilkClientRef, zk: ZooKeeperClient, actorSystem:ActorSystem)


  case class SilkClientRef(system: ActorSystem, actor: ActorRef) {
    def !(message: Any) = actor ! message
    def ?(message: Any, timeout: Timeout = 3.seconds) = {
      val future = actor.ask(message)(timeout)
      try {
        Await.result(future, timeout.duration)
      }
      catch {
        case e:TimeoutException => error(e.getMessage)
      }
    }
    def terminate {
      this ! Terminate
    }
    def close {
      // do nothing
    }
    def addr = actor.path
  }


  def remoteClient(system: ActorSystem, host: Host, clientPort: Int): ServiceGuard[SilkClientRef] = {
    val akkaAddr = s"${ActorService.AKKA_PROTOCOL}://silk@${host.address}:${clientPort}/user/SilkClient"
    trace(s"Remote SilkClient actor address: $akkaAddr")
    val actor = system.actorFor(akkaAddr)
    new ServiceGuard[SilkClientRef] {
      protected[silk] val service = new SilkClientRef(system, actor)
      def close {
        service.close
      }
    }
  }

  sealed trait ClientCommand
  case object Terminate extends ClientCommand
  case object ReportStatus extends ClientCommand
  case class Run(cbid: UUID, closure: Array[Byte])
  case object OK
}

import SilkClient._

trait SilkClientService
  extends ClusterWeaver
  with DistributedCache
  with ClusterNodeManager
  with ZooKeeperService
  with DistributedSliceStorage
  with DataServerComponent
  with LocalClientComponent
  with DistributedTaskMonitor
  with ClassBoxComponent
  with LifeCycle
  with LocalClient
  with DefaultExecutor
  with MasterRecordComponent
  with LocalTaskManagerComponent
  with MasterFinder
  with SilkActorRefFactory
  with ZkSharedStoreComponent
  with ResourceMonitorComponent
  with LocalActorServiceComponent
  with Logger
{

  val host : Host
  def currentNodeName = host.name
  def hosts = nodeManager.nodes
  def address = host.address
  def localClient = this
  val actorSystem : ActorSystem

  override def awaitTermination {
    actorSystem.awaitTermination()
  }

  override def startup {
    trace("SilkClientService start up")
    super.startup
  }

  override def teardown {
    trace("SilkClientService tear down")
    super.teardown
  }



  val localTaskManager = new LocalTaskManager {
    protected def sendToMaster(task:TaskRequest) {
      master ! task
    }
    protected def sendToMaster(taskID: UUID, status: TaskStatus) {
      master ! TaskStatusUpdate(taskID, status)
    }
  }

  override def weave[A](op:SilkSeq[A]) : SilkFuture[Seq[A]] = {
    executor.eval(op)
    new ConcreteSilkFuture(executor.run(SilkSession.defaultSession, op))
  }

  override def weave[A](op:SilkSingle[A]) : SilkFuture[A] = {
    executor.getSlices(op).head.map(slice => sliceStorage.retrieve(op.id, slice).head.asInstanceOf[A])
  }


  override private[silk] def runF0[R](locality:Seq[String], f: => R) = {
    localTaskManager.submit(classBox.classBoxID, locality)(f)
    null.asInstanceOf[R]
  }



}

/**
 * SilkClient will be deployed in each hosts and runs tasks
 *
 * @author Taro L. Saito
 */
class SilkClient(override val config:ClusterWeaver#Config, val host: Host, val zk: ZooKeeperClient, val leaderSelector: SilkMasterSelector, val dataServer: DataServer)
  extends Actor
  with SilkClientService
  with ZookeeperConnectionFailureHandler
{
  val actorSystem = context.system

  def actorRef(addr:String) = context.actorFor(addr)
  override def preStart() = {
    info(s"Start SilkClient at ${host.address}:${config.cluster.silkClientPort}")

    startup

    // Set a global reference to this SilkClient
    SilkClient.client = Some(this)

    // Register information of this machine to the ZooKeeper
    val mr = MachineResource.thisMachine
    val currentNode = Node(host.name, host.address,
      Shell.getProcessIDOfCurrentJVM,
      config.cluster.silkClientPort,
      config.cluster.dataServerPort,
      config.cluster.webUIPort,
      NodeResource(host.name, mr.numCPUs, mr.memory))
    nodeManager.addNode(currentNode)


    trace("SilkClient has started")
  }

  override def postRestart(reason: Throwable) {
    info(s"Restart the SilkClient at ${host.prefix}")
    super.postRestart(reason)
  }


  def onLostZooKeeperConnection {
    terminateServices
  }

  def terminateServices {
    leaderSelector.stop
    context.system.shutdown()
  }

  def terminate {
    terminateServices
    nodeManager.removeNode(host.name)
  }

  override def postStop() {
    info("Stopped SilkClient")
    teardown
  }


  def receive = {
    case SilkClient.ReportStatus => {
      info(s"Received status ping from ${sender.path}")
      sender ! OK
    }
    case t: TaskRequest =>
      trace(s"Accepted a task f0: ${t.id.prefix}")
      localTaskManager.execute(t.classBoxID, t)
    case r@Run(cbid, closure) => {
      info(s"received run command at $host: cb:$cbid")
      val cb = if (!dataServer.containsClassBox(cbid.prefix)) {
        val cb = classBox.getClassBox(cbid).asInstanceOf[ClassBox]
        dataServer.register(cb)
        cb
      }
      else {
        dataServer.getClassBox(cbid.prefix)
      }
      Remote.run(cb, r)
    }
    case OK => {
      info(s"Received a response OK from: $sender")
    }
    case Terminate => {
      info(s"Received a termination signal from $sender")
      sender ! OK
      terminate
    }
    case message => {
      warn(s"unknown message received: $message")
    }
  }

}



trait ZookeeperConnectionFailureHandler extends ConnectionStateListener with LifeCycle {
  self: Weaver with ZooKeeperService =>

  def onLostZooKeeperConnection : Unit

  /**
   * Called when there is a state change in the connection
   *
   * @param client the client
   * @param newState the new state
   */
  def stateChanged(client: CuratorFramework, newState: ConnectionState) {
    val logger = LoggerFactory.apply(classOf[SilkClient])
    newState match {
      case ConnectionState.LOST =>
        logger.warn("Connection to ZooKeeper is lost")
        onLostZooKeeperConnection
      case ConnectionState.SUSPENDED =>
        logger.warn("Connection to ZooKeeper is suspended")
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




