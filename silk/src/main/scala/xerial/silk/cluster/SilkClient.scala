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
import akka.dispatch.Await
import akka.util.Timeout
import akka.util.duration._
import xerial.silk.cluster.SilkMaster.{RegisterClassBox, ClassBoxHolder, AskClassBoxHolder}
import com.netflix.curator.utils.EnsurePath
import xerial.core.util.{JavaProcess, Shell}


/**
 * This class selects one of the silk clients as a SilkMaster.
 * @param zk
 * @param host
 */
private[cluster] class SilkMasterSelector(zk:CuratorFramework, host:Host) extends Logger {

  debug("Preparing SilkMaster selector")
  new EnsurePath(config.zk.leaderElectionPath).ensure(zk.getZookeeperClient)
  private val leaderSelector = new LeaderSelector(zk, config.zk.leaderElectionPath, new LeaderSelectorListener {
    private var masterSystem : Option[ActorSystem] = None
    def stateChanged(client: CuratorFramework, newState: ConnectionState) {
      if(newState == ConnectionState.LOST || newState == ConnectionState.SUSPENDED) {
        info("connection state changed: %s", newState)
        shutdownMaster
      }
    }
    def takeLeadership(client: CuratorFramework) {
      debug("Takes the leadership")
      // Start up a master client
      masterSystem = Some(SilkClient.getActorSystem(host.address, port = config.silkMasterPort))
      try {
        masterSystem map { sys =>
          sys.actorOf(Props(new SilkMaster), "SilkMaster")
          sys.awaitTermination()
        }
      }
      finally
        shutdownMaster
    }

    private def shutdownMaster {
      masterSystem map (_.shutdown)
      masterSystem = None
    }
  })

  def leaderID = leaderSelector.getId


  def start {
    // Select a master among multiple clients
    // Start the leader selector
    val id = "%s:%s".format(host.address, config.silkMasterPort)
    leaderSelector.setId(id)
    debug("master candidate id:%s", leaderSelector.getId)
    leaderSelector.autoRequeue
    leaderSelector.start()
  }

  private var isStopped = false

  def stop {
    if(!isStopped) {
      leaderSelector.close()
      isStopped = true
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
        |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
        |akka.remote.transport = "akka.remote.netty.NettyRemoteTransport"
        |akka.remote.netty.hostname = "%s"
        |akka.remote.netty.port = %d
        |      """.stripMargin.format(host, port))


    ActorSystem("silk", akkaConfig, Thread.currentThread.getContextClassLoader)
  }


  def startClient(host:Host) {

    debug("starting SilkClient...")
    ZooKeeper.withZkClient { zk =>

      val ci = ClusterCommand.getClientInfo(zk, host.name)
      // Avoid duplicate launch
      if (ci.isDefined && JavaProcess.list.find(p => p.id == ci.get.pid).isDefined) {
        info("SilkClient is already running")
        return
      }

      val newCI = ClientInfo(host, config.silkClientPort, MachineResource.thisMachine, Shell.getProcessIDOfCurrentJVM)
      info("Registering this machine to ZooKeeper: %s", newCI)
      ClusterCommand.setClientInfo(zk, host.name, newCI)

      val leaderSelector = new SilkMasterSelector(zk, host)
      leaderSelector.start

      // Start a SilkClient
      val system = getActorSystem(host.address, port = config.silkClientPort)
      try {
        val dataServer: DataServer = new DataServer(config.dataServerPort)
        val t = ThreadUtil.newManager(3)
        t.submit {
          info("Starting a new DataServer(port:%d)", config.dataServerPort)
          dataServer.start
        }
        t.submit {
          val client = system.actorOf(Props(new SilkClient(host, leaderSelector, dataServer)), "SilkClient")
          system.awaitTermination()
        }
        t.join
      }
      finally {
        system.shutdown
        leaderSelector.stop
      }
    }
  }


  private var connSystemIsStarted = false

  private lazy val connSystem = {
    val system = getActorSystem(port = IOUtil.randomPort)
    connSystemIsStarted = true
    system
  }

  def closeActorSystem {
    if(connSystemIsStarted)
      connSystem.shutdown
  }

  def withLocalClient[U](f: ActorRef => U): U = withRemoteClient(localhost.address)(f)

  def withRemoteClient[U](host: String, clientPort: Int = config.silkClientPort)(f: ActorRef => U): U = {
    val akkaAddr = "akka://silk@%s:%s/user/SilkClient".format(host, clientPort)
    trace("Remote SilkClient actor address: %s", akkaAddr)
    val actor = connSystem.actorFor(akkaAddr)
    f(actor)
  }

  sealed trait ClientCommand
  case object Terminate extends ClientCommand
  case object Status extends ClientCommand

  case class ClientInfo(host:Host, port:Int, m: MachineResource, pid: Int)
  case class Run(classBoxID: String, closure: Array[Byte])
  case class Register(cb: ClassBox)

  case object OK

}




import SilkClient._

/**
 * SilkClient run the jobs
 *
 * @author Taro L. Saito
 */
class SilkClient(host:Host, leaderSelector:SilkMasterSelector, dataServer:DataServer) extends Actor with Logger {


  private var master : ActorRef = null
  private val timeout = 1 seconds

  override def preStart() = {
    info("Start SilkClient at %s:%d", host.address, config.silkClientPort)

    // Get an ActorRef of the SilkMaster
    try {
      val masterAddr = "akka://silk@%s/user/SilkMaster".format(leaderSelector.leaderID)
      debug("Remote SilkMaster address: %s, host:%s", masterAddr, host)
      master = context.actorFor(masterAddr)
      master ! Status
    }
    catch {
      case e:Exception =>
        error(e)
        terminate
    }
  }


  override def postRestart(reason: Throwable) {
    info("Restart the SilkClient at %s", host.prefix)
    super.postRestart(reason)
  }

  protected def receive = {
    case Terminate => {
      debug("Recieved a termination signal")
      sender ! OK

      terminate
    }
    case Status => {
      info("Recieved status ping")
      sender ! OK
    }
    case r@Run(cbid, closure) => {
      info("recieved run command at %s: cb:%s", host, cbid)
      if(!dataServer.containsClassBox(cbid)) {
        debug("Retrieving classbox")
        val future = master.ask(AskClassBoxHolder(cbid))(timeout)
        val ret = Await.result(future, timeout)
        ret match {
          case cbh:ClassBoxHolder =>
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
      if(!dataServer.containsClassBox(cb.id)) {
        info("Register a ClassBox %s to the local DataServer", cb.sha1sum)
        dataServer.register(cb)
        master ! RegisterClassBox(cb, ClientAddr(host, config.dataServerPort))
      }
    }
    case message => {
      warn("unknown message recieved: %s", message)
    }
  }

  private def terminate {
    context.stop(self)
    dataServer.stop
    context.system.shutdown()
    leaderSelector.stop
  }

  override def postStop() {
    info("Stopped SilkClient")
  }

}


