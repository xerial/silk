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
import xerial.core.io.IOUtil
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
import java.io._
import xerial.silk.cluster.SilkMaster._
import java.util.concurrent.TimeoutException
import xerial.silk.util.ThreadUtil.ThreadManager
import xerial.silk.cluster.SilkMaster.RegisterClassBox
import xerial.silk.cluster.SilkMaster.AskClassBoxHolder
import xerial.silk.cluster.SilkMaster.DataHolder
import xerial.silk.cluster.SilkMaster.AskDataHolder
import scala.Some
import xerial.silk.cluster.SilkMaster.ClassBoxHolder
import xerial.silk.cluster.SilkMaster.RegisterDataInfo
import xerial.silk.cluster.framework.{ZooKeeperService, ClusterNodeManager, SilkClientService, ActorService}
import xerial.silk.framework.{DefaultConsoleLogger, NodeResource, Node, SilkFramework}

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


/**
 * SilkClient is a network interface that accepts command from the other hosts
 */
object SilkClient extends Logger {


  private[cluster] var client : Option[SilkClient] = None
  val dataTable = collection.mutable.Map[String, AnyRef]()

  def startClient[U](host:Host, zkConnectString:String)(f: SilkClientRef => U) : Unit = {

    info(s"Start SilkClient at $host, zk:$zkConnectString")

    for (zkc <- ZooKeeper.zkClient(zkConnectString) whenMissing {
      warn("No Zookeeper appears to be running. Run 'silk cluster start' first.")
    }) {

      val clusterManager = new ClusterNodeManager with ZooKeeperService {
        val zk : ZooKeeperClient = zkc
      }

      if(clusterManager.clientIsActive(host.name)) {
        // Avoid duplicate launch
        info("SilkClient is already running")
      }
      else {
        val leaderSelector = new SilkMasterSelector(zkc, host)
        leaderSelector.start

        // Start a SilkClient
        val tm = new ThreadManager(2)
        val system = ActorService.getActorSystem(host.address, port = config.silkClientPort)
        val dataServer: DataServer = new DataServer(config.dataServerPort)
        val clientRef = new SilkClientRef(system, system.actorOf(Props(new SilkClient(host, zkc, leaderSelector, dataServer)), "SilkClient"))
        try {

          // Start a data server
          tm.submit {
            info(s"Start a new DataServer(port:${config.dataServerPort})")
            dataServer.start
          }
          tm.submit {
            // Await the termination of the Actor system.
            system.awaitTermination()
          }

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
          debug("SilkClient is ready")
          // exec user code
          f(clientRef)
        }
        catch {
          case e:Exception => warn(e)
        }
        finally {
          debug("Self-termination phase")
          clientRef ! Terminate
          //dataServer.stop
          //leaderSelector.stop
          tm.join // wait until DataServer and ActorSystem have finished
          system.shutdown()
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
    val system = ActorService.getActorSystem(port = IOUtil.randomPort)
    val akkaAddr = s"${ActorService.AKKA_PROTOCOL}://silk@${host.address}:${clientPort}/user/SilkClient"
    trace(s"Remote SilkClient actor address: $akkaAddr")
    val actor = system.actorFor(akkaAddr)
    ConnectionWrap(new SilkClientRef(system, actor))
  }

  private def withLocalClient[U](f: ActorRef => U): U = withRemoteClient(localhost.address)(f)

  private def withRemoteClient[U](host: String, clientPort: Int = config.silkClientPort)(f: ActorRef => U): U = {
    val system = ActorService.getActorSystem(port = IOUtil.randomPort)
    try {
      val akkaAddr = s"${ActorService.AKKA_PROTOCOL}://silk@%s:%s/user/SilkClient".format(host, clientPort)
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

  case object GetPort
  case class ClientInfo(host: Host, port: Int, dataServerPort:Int, m: MachineResource, pid: Int) {
    def name = host.name
  }
  case class Run(classBoxID: String, closure: Array[Byte])
  case class RegisterClassBox(cb: ClassBox)
  case class DownloadDataFrom(host:Host, port:Int, filePath:File, offset:Long, size:Long)
  case class RegisterFile(file:File)
  case class DataReference(id: String, host: Host, port: Int)
  case class RegisterData(args: DataReference)
  case class GetDataInfo(id: String)
  case class ExecuteFunction0[A](function: Function0[A])
  case class ExecuteFunction1[A, B](function: Function1[A, B], argsID: String, resultID: String)
  case class ExecuteFunction2[A, B, C](function: Function2[A, B, C], argsID: String, resultID: String)
  case class ExecuteFunction3[A, B, C, D](function: Function3[A, B, C, D], argsID: String, resultID: String)

  case object OK


  private[cluster] def getClientInfo(zk: ZooKeeperClient, hostName: String): Option[ClientInfo] = {
    val data = zk.get(config.zk.clientEntryPath(hostName))
    data flatMap { b =>
      try
        Some(SilkSerializer.deserializeAny(b).asInstanceOf[ClientInfo])
      catch {
        case e : Exception =>
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
class SilkClient(val host: Host, val zk: ZooKeeperClient, val leaderSelector: SilkMasterSelector, val dataServer: DataServer)
  extends Actor
  with SilkClientService
{

  private var master: ActorRef = null
  private val timeout = 3.seconds

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

    startUp

    SilkClient.client = Some(this)

    val mr = MachineResource.thisMachine
    val currentNode = Node(host.name, host.address,
      Shell.getProcessIDOfCurrentJVM,
      config.silkClientPort,
      config.dataServerPort,
      NodeResource(host.name, mr.numCPUs, mr.memory))
    nodeManager.addNode(currentNode)


    // Get an ActorRef of the SilkMaster
    try {
      val masterAddr = s"${ActorService.AKKA_PROTOCOL}://silk@${leaderSelector.leaderID}/user/SilkMaster"
      debug(s"Remote SilkMaster address: $masterAddr, host:$host")

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

    debug("SilkClient has started")
  }

  override def postRestart(reason: Throwable) {
    info(s"Restart the SilkClient at ${host.prefix}")
    super.postRestart(reason)
  }

  def receive = {
    case Terminate => {
      debug("Recieved a termination signal")
      sender ! OK
      terminate
    }
    case SilkClient.ReportStatus => {
      debug(s"Recieved status ping from ${sender.path}")
      sender ! OK
    }
    case RegisterFile(file) => {
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
      sender ! OK
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
    case SilkClient.RegisterClassBox(cb) => {
      if (!dataServer.containsClassBox(cb.id)) {
        info(s"RegisterClassBox a ClassBox ${cb.sha1sum} to the local DataServer")
        dataServer.register(cb)
        val future = master.ask(RegisterClassBox(cb, ClientAddr(host, config.dataServerPort)))(timeout)
        val ret = Await.result(future, timeout)
        ret match {
          case OK => info(s"Registred ClassBox ${cb.id} to the SilkMaster")
          case e => warn(s"timeout: ${e}")
        }
      }
    }
    case RegisterData(argsInfo) =>
    {
      val future = master.ask(RegisterDataInfo(argsInfo.id, DataAddr(argsInfo.host, argsInfo.port)))(timeout)
      Await.result(future, timeout) match
      {
        case OK => info(s"Registered information of data ${argsInfo.id} to the SilkMaster")
        case e => warn(s"timeout: ${e}")
      }
    }
    case ExecuteFunction0(func) => func()
    case ExecuteFunction1(func, argsID, resID) =>
    {
      val future = master.ask(AskDataHolder(argsID))(timeout)
      Await.result(future, timeout) match
      {
        case DataNotFound(id) => warn(s"Data request ${id} is not found.")
        case DataHolder(id, holder) =>
        {
          val dataURL = new URL(s"http://${holder.host.address}:${holder.port}/data/${id}")
          info(s"Accessing ${dataURL.toString}")
          IOUtil.readFully(dataURL.openStream())
          {
            arguments =>
              val ois = new ObjectInputStream(new ByteArrayInputStream(arguments))
              val args = ois.readObject.asInstanceOf[Product1[Nothing]]
              for (method <- func.getClass.getDeclaredMethods.find(m => m.getName == "apply" && !m.isSynthetic))
              {
                val retType = method.getReturnType
                retType match
                {
                  case t if t == classOf[Unit] => func(args._1)
                  case _ =>
                    val result = func(args._1)
                    val serializedObject = serializeObject(result)
                    dataServer.register(resID, serializedObject)
                    val dr = new DataReference(resID, host, client.map(_.dataServer.port).get)
                    self ! RegisterData(dr)
                }
              }
          }
        }
      }
    }
    case ExecuteFunction2(func, argsID, resID) =>
    {
      val future = master.ask(AskDataHolder(argsID))(timeout)
      Await.result(future, timeout) match
      {
        case DataNotFound(id) => warn(s"Data request ${id} is not found.")
        case DataHolder(id, holder) =>
        {
          val dataURL = new URL(s"http://${holder.host.address}:${holder.port}/data/${id}")
          info(s"Accessing ${dataURL.toString}")
          IOUtil.readFully(dataURL.openStream())
          {
            arguments =>
              val ois = new ObjectInputStream(new ByteArrayInputStream(arguments))
              val args = ois.readObject().asInstanceOf[Product2[Nothing, Nothing]]
              func(args._1, args._2)
          }
        }
      }
    }
    case ExecuteFunction3(func, argsID, resID) =>
    {
      val future = master.ask(AskDataHolder(argsID))(timeout)
      Await.result(future, timeout) match
      {
        case DataNotFound(id) => warn(s"Argument request ${id} is not found.")
        case DataHolder(id, holder) =>
        {
          val dataURL = new URL(s"http://${holder.host.address}:${holder.port}/data/${id}")
          info(s"Accessing ${dataURL.toString}")
          IOUtil.readFully(dataURL.openStream)
          {
            arguments =>
              val ois = new ObjectInputStream(new ByteArrayInputStream(arguments))
              val args = ois.readObject.asInstanceOf[Product3[Nothing, Nothing, Nothing]]
              for (method <- func.getClass.getDeclaredMethods.find(m => m.getName == "apply" && !m.isSynthetic))
              {
                val retType = method.getReturnType
                retType match
                {
                  case t if t == classOf[Unit] => func(args._1, args._2, args._3)
                  case _ =>
                    val result = func(args._1, args._2, args._3)
                    val serializedObject = serializeObject(result)
                    dataServer.register(resID, serializedObject)
                    val dr = new DataReference(resID, host, client.map(_.dataServer.port).get)
                    self ! RegisterData(dr)
                }
              }
          }
        }
      }
    }
    case GetDataInfo(id) =>
    {
      val future = master.ask(AskDataHolder(id))(timeout)
      Await.result(future, timeout) match
      {
        case DataNotFound(id) =>
        {
          warn(s"Data request ${id} is not found.")
          sender ! DataNotFound(id)
        }
        case DataHolder(id, holder) =>
        {
          info(s"Sending data $id info.")
          sender ! DataHolder(id, holder)
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
    context.system.shutdown()
    leaderSelector.stop
    nodeManager.removeNode(host.name)
  }

  override def postStop() {
    info("Stopped SilkClient")
    tearDown
  }
}


