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

import com.typesafe.config.ConfigFactory
import akka.actor.{ActorRef, Props, Actor, ActorSystem}
import xerial.core.log.Logger
import xerial.core.io.{IOUtil}
import xerial.silk.util.ThreadUtil
import com.netflix.curator.framework.recipes.leader.{LeaderSelectorListener, LeaderSelector}
import com.netflix.curator.framework.CuratorFramework
import com.netflix.curator.framework.state.ConnectionState
import akka.pattern.ask
import scala.concurrent.Await
import akka.util.Timeout
import scala.concurrent.duration._
import xerial.silk.cluster.SilkMaster.{RegisterClassBox, ClassBoxHolder, AskClassBoxHolder}
import com.netflix.curator.utils.EnsurePath
import xerial.core.util.{JavaProcess, Shell}
import xerial.silk.util.ThreadUtil.ThreadManager
import xerial.silk.core.SilkSerializer


/**
 * This class selects one of the silk clients as a SilkMaster.
 * @param zk
 * @param host
 */
private[cluster] class SilkMasterSelector(zk: ZooKeeperClient, host: Host) extends Logger {

  @volatile private var masterSystem: Option[ActorSystem] = None

  debug("Preparing SilkMaster selector")
  zk.makePath(config.zk.leaderElectionPath)
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
          info("connection state changed: %s", newState)
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
        masterSystem = Some(SilkClient.getActorSystem(host.address, port = config.silkMasterPort))
        try {
          masterSystem map {
            sys =>
              sys.actorOf(Props(new SilkMaster), "SilkMaster")
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
    ///debug("master candidate id:%s", leaderSelector.getId)
    //leaderSelector.autoRequeue
    leaderSelector.map(_.start())
    isStarted = true
  }

  private var isStarted = false
  private var isStopped = false

  def stop {
    if (isStarted && !isStopped) {
      synchronized {
        info("Closing SilkMasterSelector")
        leaderSelector.map(_.close())
        isStopped = true
      }
    }

  }

}


/**
 * SilkClient is a network interface that accepts command from the other hosts
 */
object SilkClient extends Logger {

  def getActorSystem(host: String = localhost.address, port: Int) = {
    debug("Creating an actor system using %s:%d", host, port)
    val akkaConfig = ConfigFactory.parseString(
      """
        |akka.daemonic = on
        |akka.event-handlers = ["akka.event.Logging$DefaultLogger"]
        |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
        |akka.remote.transport = "akka.remote.netty.NettyRemoteTransport"
        |akka.remote.netty.connection-timeout = 15s
        |akka.remote.netty.hostname = "%s"
        |akka.remote.netty.port = %d
        |      """.stripMargin.format(host, port))


    ActorSystem("silk", akkaConfig, Thread.currentThread.getContextClassLoader)
  }


  def startClient(host: Host, zkConnectString:String) {

    debug("starting SilkClient...")

    for (zk <- ZooKeeper.zkClient(zkConnectString) whenMissing { warn("No Zookeeper appears to be running. Run 'silk cluster start' first.") } ) {
      val isRunning = {
        val ci = getClientInfo(zk, host)
        // Avoid duplicate launch
        if (ci.isDefined && JavaProcess.list.find(p => p.id == ci.get.pid).isDefined) {
          info("SilkClient is already running")
          registerToZK(zk, host)
          true
        }
        else
          false
      }

      if (!isRunning) {
        val leaderSelector = new SilkMasterSelector(zk, host)
        leaderSelector.start

        // Start a SilkClient
        val system = getActorSystem(host.address, port = config.silkClientPort)
        try {
          val dataServer: DataServer = new DataServer(config.dataServerPort)
          val t = ThreadUtil.newManager(2)
          t.submit {
            info("Starting a new DataServer(port:%d)", config.dataServerPort)
            dataServer.start
          }
          t.submit {
            val client = system.actorOf(Props(new SilkClient(host, zk, leaderSelector, dataServer)), "SilkClient")
            system.awaitTermination()
            info("exit awaitTermination")
          }
          t.join
        }
        finally {
          leaderSelector.stop
          info("Terminates the actor system for SilkClient")
          system.shutdown
          //closeActorSystem
        }
      }
    }

    info("SilkClient stopped")
  }


  //  private var connSystemIsStarted = false
  //
  //  private val connSystem = {
  //    val system = getActorSystem(port = IOUtil.randomPort)
  //    connSystemIsStarted = true
  //    system
  //  }
  //
  //  def closeActorSystem {
  //    if (connSystemIsStarted) {
  //      info("Terminates the actor system for local clients")
  //      connSystem.shutdown
  //    }
  //  }


  class SilkClientRef(system:ActorSystem, actor:ActorRef) {
    def ! (message:Any) = actor ! message
    def ? (message:Any, timeout:Timeout = 3 seconds) = {
      val future = actor.ask(message)(timeout)
      Await.result(future, timeout.duration)
    }
    def close {
      system.shutdown
    }
    def addr = actor.path
  }

  def localClient = remoteClient(localhost)
  def remoteClient(host:Host, clientPort:Int = config.silkClientPort) : ConnectionWrap[SilkClientRef] = {
    val system = getActorSystem(port = IOUtil.randomPort)
    val akkaAddr = "akka://silk@%s:%s/user/SilkClient".format(host.address, clientPort)
    trace("Remote SilkClient actor address: %s", akkaAddr)
    val actor = system.actorFor(akkaAddr)
    ConnectionWrap(new SilkClientRef(system, actor))
  }

  private def withLocalClient[U](f: ActorRef => U): U = withRemoteClient(localhost.address)(f)

  private def withRemoteClient[U](host: String, clientPort: Int = config.silkClientPort)(f: ActorRef => U): U = {
    val system = getActorSystem(port = IOUtil.randomPort)
    try {
      val akkaAddr = "akka://silk@%s:%s/user/SilkClient".format(host, clientPort)
      trace("Remote SilkClient actor address: %s", akkaAddr)
      val actor = system.actorFor(akkaAddr)
      f(actor)
    }
    finally {
      system.shutdown
    }
  }

  sealed trait ClientCommand
  case object Terminate extends ClientCommand
  case object Status extends ClientCommand

  case class ClientInfo(host: Host, port: Int, m: MachineResource, pid: Int)
  case class Run(classBoxID: String, closure: Array[Byte])
  case class Register(cb: ClassBox)

  case object OK


  private[SilkClient] def registerToZK(zk: ZooKeeperClient, host: Host) {
    val newCI = ClientInfo(host, config.silkClientPort, MachineResource.thisMachine, Shell.getProcessIDOfCurrentJVM)
    info("Registering this machine to ZooKeeper: %s", newCI)
    zk.set(config.zk.clientEntryPath(host.name), SilkSerializer.serialize(newCI))
  }
  private[SilkClient] def unregisterFromZK(zk:ZooKeeperClient, host:Host) {
    zk.remove(config.zk.clientEntryPath(host.name))
  }


  private[cluster] def getClientInfo(zk: ZooKeeperClient, host: Host): Option[ClientInfo] = {
    val data = zk.get(config.zk.clientEntryPath(host.name))
    data flatMap { b =>
      try
        Some(SilkSerializer.deserializeAny(b).asInstanceOf[ClientInfo])
      catch {
        case e : Throwable =>
          warn(e)
          None
      }
    }
  }


}


import SilkClient._

/**
 * SilkClient run the jobs
 *
 * @author Taro L. Saito
 */
class SilkClient(host: Host, zk: ZooKeeperClient, leaderSelector: SilkMasterSelector, dataServer: DataServer) extends Actor with Logger {


  private var master: ActorRef = null
  private val timeout = 3 seconds

  override def preStart() = {
    info("Start SilkClient at %s:%d", host.address, config.silkClientPort)

    registerToZK(zk, host)

    // Get an ActorRef of the SilkMaster
    try {
      val masterAddr = "akka://silk@%s/user/SilkMaster".format(leaderSelector.leaderID)
      info("Remote SilkMaster address: %s, host:%s", masterAddr, host)
      master = context.actorFor(masterAddr)
      master ! Status
    }
    catch {
      case e: Throwable =>
        error(e)
        terminate
    }
  }


  override def postRestart(reason: Throwable) {
    info("Restart the SilkClient at %s", host.prefix)
    super.postRestart(reason)
  }

  def receive = {
    case Terminate => {
      info("Recieved a termination signal")
      sender ! OK
      terminate
    }
    case Status => {
      info("Recieved status ping")
      sender ! OK
    }
    case r@Run(cbid, closure) => {
      info("recieved run command at %s: cb:%s", host, cbid)
      if (!dataServer.containsClassBox(cbid)) {
        debug("Retrieving classbox")
        val future = master.ask(AskClassBoxHolder(cbid))(timeout)
        val ret = Await.result(future, timeout)
        ret match {
          case cbh: ClassBoxHolder =>
            debug("response from Master:%s", cbh)
            val cb = ClassBox.sync(cbh.cb, cbh.holder)
            dataServer.register(cb)
            Remote.run(dataServer.getClassBox(cbid), r)
          case other => {
            warn("ClassBox %s is not found in Master: %s", cbid, other)
          }
        }
      }
      else
        Remote.run(dataServer.getClassBox(cbid), r)
    }
    case Register(cb) => {
      if (!dataServer.containsClassBox(cb.id)) {
        info("Register a ClassBox %s to the local DataServer", cb.sha1sum)
        dataServer.register(cb)
        master ! RegisterClassBox(cb, ClientAddr(host, config.dataServerPort))
      }
    }
    case OK => {
      info("Recieved a response OK from: %s", sender)
    }
    case message => {
      warn("unknown message recieved: %s", message)
    }
  }

  private def terminate {
    dataServer.stop
    leaderSelector.stop
    context.stop(self)
    context.system.shutdown()
    unregisterFromZK(zk, host)
  }

  override def postStop() {
    info("Stopped SilkClient")
  }

}


