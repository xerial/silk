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
import akka.actor._
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
import xerial.core.util.{JavaProcess, Shell}
import xerial.silk.core.SilkSerializer
import java.net.URL
import java.io.File
import xerial.silk.cluster.SilkMaster.RegisterClassBox
import xerial.silk.cluster.SilkMaster.AskClassBoxHolder
import xerial.silk.cluster.SilkMaster.ClassBoxHolder
import java.util.concurrent.TimeoutException
import xerial.silk.util.ThreadUtil.ThreadManager


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

  private[cluster] val AKKA_PROTOCOL = "akka"

  def getActorSystem(host: String = localhost.address, port: Int) = {
    debug(s"Creating an actor system using $host:$port")
    val akkaConfig = ConfigFactory.parseString(
      """
        |akka.loglevel = "ERROR"
        |akka.daemonic = on
        |akka.event-handlers = ["akka.event.Logging$DefaultLogger"]
        |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
        |akka.remote.transport = "akka.remote.netty.NettyRemoteTransport"
        |akka.remote.netty.connection-timeout = 15s
        |akka.remote.netty.hostname = "%s"
        |akka.remote.netty.port = %d
        |      """.stripMargin.format(host, port))

//    /
//    |akka.remote.enabled-transports = ["akka.remote.netty.tcp"]
//    |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
//    |akka.remote.netty.tcp.connection-timeout = 15s
//      |akka.remote.netty.tcp.hostname c= "%s"
//    |akka.remote.netty.tcp.port = %d

    //|akka.log-config-on-start = on
    //|akka.actor.serialize-messages = on
    //|akka.actor.serialize-creators = on


    //|akka.loggers = ["akka.event.Logging$DefaultLogger"]
    ActorSystem("silk", akkaConfig, Thread.currentThread.getContextClassLoader)
  }

  def startClient[U](host:Host, zkConnectString:String)(f: SilkClientRef => U) : Unit = {

    debug("starting SilkClient...")

    for (zk <- ZooKeeper.zkClient(zkConnectString) whenMissing {
      warn("No Zookeeper appears to be running. Run 'silk cluster start' first.")
    }) {
      val isRunning = {
        val ci = getClientInfo(zk, host.name)
        // Avoid duplicate launch
        val jps = JavaProcess.list
        ci match {
          case Some(c) if jps.exists(_.id == c.pid) && c.port == config.silkClientPort =>
            info("SilkClient is already running")
            // Re-register myself to the ZooKeeper
            registerToZK(zk, host)
            true
          case _ =>
            false
        }
      }

      if (!isRunning) {
        val leaderSelector = new SilkMasterSelector(zk, host)
        leaderSelector.start

        // Start a SilkClient
        val system = getActorSystem(host.address, port = config.silkClientPort)
        val dataServer: DataServer = new DataServer(config.dataServerPort)
        val clientRef = new SilkClientRef(system, system.actorOf(Props(new SilkClient(host, zk, leaderSelector, dataServer)), "SilkClient"))
        try {

          val t = new Thread(new Runnable{
            def run() {
              info(s"Starting a new DataServer(port:${config.dataServerPort})")
              dataServer.start
            }
          })
          t.setDaemon(true)
          t.start
 
          // Wait until the client has started
          val maxRetry = 10
          var retry = 0
          var clientIsReady = false
          while(!clientIsReady && retry < maxRetry) {
            try {
              val result = clientRef ? (ReportStatus)
              result match {
                case OK => clientIsReady = true
              }
            }
            catch {
              case e: TimeoutException =>
                retry += 1
            }
          }
          info("SilkClient is ready")
          // exec user code
          f(clientRef)
        }
        finally {
          clientRef ! Terminate
          system.awaitTermination()
          system.shutdown()
          leaderSelector.stop
          dataServer.stop
        }
      }
    }

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

  def localClient = remoteClient(localhost)

  def remoteClient(ci:ClientInfo): ConnectionWrap[SilkClientRef] = remoteClient(ci.host, ci.port)

  def remoteClient(host: Host, clientPort: Int = config.silkClientPort): ConnectionWrap[SilkClientRef] = {
    val system = getActorSystem(port = IOUtil.randomPort)
    val akkaAddr = s"${AKKA_PROTOCOL}://silk@${host.address}:${clientPort}/user/SilkClient"
    trace(s"Remote SilkClient actor address: $akkaAddr")
    val actor = system.actorFor(akkaAddr)
    ConnectionWrap(new SilkClientRef(system, actor))
  }

  private def withLocalClient[U](f: ActorRef => U): U = withRemoteClient(localhost.address)(f)

  private def withRemoteClient[U](host: String, clientPort: Int = config.silkClientPort)(f: ActorRef => U): U = {
    val system = getActorSystem(port = IOUtil.randomPort)
    try {
      val akkaAddr = s"${AKKA_PROTOCOL}://silk@%s:%s/user/SilkClient".format(host, clientPort)
      debug(s"Remote SilkClient actor address: $akkaAddr")
      val actor = system.actorFor(akkaAddr)
      f(actor)
    }
    finally {
      system.shutdown
    }
  }

  sealed trait ClientCommand
  case object Terminate extends ClientCommand
  case object ReportStatus extends ClientCommand

  case class ClientInfo(host: Host, port: Int, dataServerPort:Int, m: MachineResource, pid: Int)
  case class Run(classBoxID: String, closure: Array[Byte])
  case class Register(cb: ClassBox)
  case class DownloadDataFrom(host:Host, port:Int, filePath:File, offset:Long, size:Long)
  case class RegisterData(file:File)

  case object OK


  private[SilkClient] def registerToZK(zk: ZooKeeperClient, host: Host) {
    val newCI = ClientInfo(host, config.silkClientPort, config.dataServerPort, MachineResource.thisMachine, Shell.getProcessIDOfCurrentJVM)
    info(s"Registering this machine to ZooKeeper: $newCI")
    zk.set(config.zk.clientEntryPath(host.name), SilkSerializer.serialize(newCI))
  }
  private[SilkClient] def unregisterFromZK(zk: ZooKeeperClient, host: Host) {
    zk.remove(config.zk.clientEntryPath(host.name))
  }


  private[cluster] def getClientInfo(zk: ZooKeeperClient, hostName: String): Option[ClientInfo] = {
    val data = zk.get(config.zk.clientEntryPath(hostName))
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
class SilkClient(val host: Host, zk: ZooKeeperClient, leaderSelector: SilkMasterSelector, val dataServer: DataServer) extends Actor with Logger {


  private var master: ActorRef = null
  private val timeout = 3.seconds

  override def preStart() = {
    info(s"Start SilkClient at ${host.address}:${config.silkClientPort}")

    registerToZK(zk, host)

    // Get an ActorRef of the SilkMaster
    try {
      val masterAddr = s"${AKKA_PROTOCOL}://silk@%s/user/SilkMaster".format(leaderSelector.leaderID)
      info(s"Remote SilkMaster address: $masterAddr, host:$host")

      // wait until the master is ready
      val maxRetry = 10
      var retry = 0
      var masterIsReady = false
      while(!masterIsReady && retry < maxRetry) {
        try {
          master = context.actorFor(masterAddr)
          val ret = master.ask(SilkClient.ReportStatus)(timeout)
          Await.result(ret, timeout)
          masterIsReady = true
        }
        catch {
          case e:TimeoutException =>
            retry += 1
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

    info("SilkClient has started")
  }


  override def postRestart(reason: Throwable) {
    info(s"Restart the SilkClient at ${host.prefix}")
    super.postRestart(reason)
  }

  def receive = {
    case Terminate => {
      info("Recieved a termination signal")
      sender ! OK
      terminate
    }
    case SilkClient.ReportStatus => {
      info(s"Recieved status ping from ${sender.path}")
      sender ! OK
    }
    case RegisterData(file) => {
      // TODO use hash value of data as data ID or UUID
      warn(s"register data $file")
      dataServer.registerData(file.getName, file)
    }
    case DownloadDataFrom(host, port, fileName, offset, size) => {
      val dataURL = new URL(s"http://${host.address}:${port}/data/${fileName.getName}:${offset}:${size}")
      warn(s"download data from $dataURL")
      IOUtil.readFully(dataURL.openStream()) { result =>
        debug(s"result: ${result.map(e => f"$e%x").mkString(" ")}")
      }
      // TODO how to use the obtained result?
    }
    case r@Run(cbid, closure) => {
      info(s"recieved run command at $host: cb:$cbid")
      if (!dataServer.containsClassBox(cbid)) {
        debug("Retrieving classbox")
        val future = master.ask(AskClassBoxHolder(cbid))(timeout)
        val ret = Await.result(future, timeout)
        ret match {
          case cbh: ClassBoxHolder =>
            debug(s"response from Master:$cbh")
            val cb = ClassBox.sync(cbh.cb, cbh.holder)
            dataServer.register(cb)
            Remote.run(dataServer.getClassBox(cbid), r)
          case e => {
            warn(s"ClassBox $cbid is not found in Master: $e")
          }
        }
      }
      else
        Remote.run(dataServer.getClassBox(cbid), r)
    }
    case Register(cb) => {
      if (!dataServer.containsClassBox(cb.id)) {
        info(s"Register a ClassBox ${cb.sha1sum} to the local DataServer")
        dataServer.register(cb)
        val future = master.ask(RegisterClassBox(cb, ClientAddr(host, config.dataServerPort)))(timeout)
        val ret = Await.result(future, timeout)
        ret match {
          case OK => info(s"Registred ClassBox ${cb.id} to the SilkMaster")
          case e => warn(s"timeout: ${e}")
        }
      }
    }
    case OK => {
      info(s"Recieved a response OK from: $sender")
    }
    case message => {
      warn(s"unknown message recieved: $message")
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


