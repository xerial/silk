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
import xerial.core.log.Logger
import xerial.core.io.IOUtil
import akka.pattern.ask
import scala.concurrent.Await
import akka.util.Timeout
import scala.concurrent.duration._
import xerial.core.util.{JavaProcess, Shell}
import java.net.URL
import java.io._
import java.util.concurrent.TimeoutException
import xerial.silk.util.ThreadUtil.ThreadManager
import xerial.silk.cluster.framework.{ZooKeeperService, ClusterNodeManager, SilkClientService, ActorService}
import xerial.silk.framework._
import java.util.UUID
import xerial.silk.io.ServiceGuard
import xerial.silk.cluster._


/**
 * SilkClient is a network interface that accepts command from the other hosts
 */
object SilkClient extends Logger {


  private[silk] var client : Option[SilkClient] = None
  val dataTable = collection.mutable.Map[String, AnyRef]()


  case class ClientEnv(clientRef:SilkClientRef, zk:ZooKeeperClient)




  case class SilkClientRef(system: ActorSystem, actor: ActorRef) {
    def !(message: Any) = actor ! message
    def ?(message: Any, timeout: Timeout = 3.seconds) = {
      val future = actor.ask(message)(timeout)
      Await.result(future, timeout.duration)
    }
    def terminate {
      this ! Terminate
    }
    def close {
      system.shutdown
    }
    def addr = actor.path
  }


  def remoteClient(system:ActorSystem, host: Host, clientPort: Int = config.silkClientPort): ServiceGuard[SilkClientRef] = {
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
import SilkMaster._

/**
 * SilkClient run the jobs
 *
 * @author Taro L. Saito
 */
class SilkClient(val host: Host, val zk: ZooKeeperClient, val leaderSelector: SilkMasterSelector, val dataServer: DataServer)
  extends Actor
  with SilkClientService
{
  //type LocalClient = SilkClient
  def localClient = this
  def address = host.address

  var master: ActorRef = null
  private val timeout = 10.seconds

  private def serializeObject[A](obj: A): Array[Byte] =
  {
    val baos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(obj)
    oos.close
    baos.toByteArray
  }

  override def preStart() = {
    info(s"Start SilkClient at ${host.address}:${config.silkClientPort}")

    startup

    SilkClient.client = Some(this)

    val mr = MachineResource.thisMachine
    val currentNode = Node(host.name, host.address,
      Shell.getProcessIDOfCurrentJVM,
      config.silkClientPort,
      config.dataServerPort,
      config.webUIPort,
      NodeResource(host.name, mr.numCPUs, mr.memory))
    nodeManager.addNode(currentNode)


    // Get an ActorRef of the SilkMaster
    try {
      val mr = getOrAwaitMaster.get
      val masterAddr = s"${ActorService.AKKA_PROTOCOL}://silk@${mr.address}:${mr.port}/user/SilkMaster"
      info(s"Connecting to SilkMaster: $masterAddr, master host:${mr.name}")

      // wait until the master is ready
      var timeout = 10.0
      val maxRetry = 10
      var retry = 0
      var masterIsReady = false
      while(!masterIsReady && retry < maxRetry) {
        try {
          master = context.actorFor(masterAddr)
          val ret = master.ask(SilkClient.ReportStatus)(timeout.seconds)
          Await.result(ret, timeout.seconds)
          masterIsReady = true
        }
        catch {
          case e:TimeoutException =>
            retry += 1
            timeout += timeout * 1.5
        }
      }
      if(!masterIsReady) {
        error("Failed to find SilkMaster")
        terminate
      }
    }
    catch {
      case e: Exception =>
        error(e)
        terminate
    }

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
      info(s"Recieved status ping from ${sender.path}")
      sender ! OK
    }
    case t : TaskRequest =>
      trace(s"Accepted a task f0: ${t.id.prefix}")
      localTaskManager.execute(t.classBoxID, t)
    case r@Run(cbid, closure) => {
      info(s"recieved run command at $host: cb:$cbid")
      val cb = if (!dataServer.containsClassBox(cbid.prefix)) {
        val cb = getClassBox(cbid).asInstanceOf[ClassBox]
        dataServer.register(cb)
        cb
      }
      else {
        dataServer.getClassBox(cbid.prefix)
      }
      Remote.run(cb, r)
    }
    case OK => {
      info(s"Recieved a response OK from: $sender")
    }
    case Terminate => {
      warn("Recieved a termination signal")
      sender ! OK
      terminate
    }
    case message => {
      warn(s"unknown message recieved: $message")
    }
  }

}


