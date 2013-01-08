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
import xerial.core.util.Shell


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


  def startClient = {
    debug("starting SilkClient...")
    ZooKeeper.withZkClient {
      zk =>
        val jvmPID = Shell.getProcessIDOfCurrentJVM
        val m = MachineResource.thisMachine
        info("Registering this machine to ZooKeeper")
        ClusterCommand.setClientInfo(zk, localhost.name, ClientInfo(m, jvmPID))

        // Select a master among multiple clients
        debug("Preparing SilkMaster selector")
        new EnsurePath(config.zk.leaderElectionPath).ensure(zk.getZookeeperClient)
        val leaderSelector = new LeaderSelector(zk, config.zk.leaderElectionPath, new LeaderSelectorListener {
          private var masterSystem : Option[ActorSystem] = None
          def stateChanged(client: CuratorFramework, newState: ConnectionState) {
            if(newState == ConnectionState.LOST || newState == ConnectionState.SUSPENDED)
              shutdownMaster
          }
          def takeLeadership(client: CuratorFramework) {
            debug("Takes leadership")
            // Start up a master client
            masterSystem = Some(getActorSystem(port = config.silkMasterPort))
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
            synchronized {
              masterSystem map (_.shutdown)
              masterSystem = None
            }
          }
        })

        // Start the leader selector
        val id = "%s:%s".format(localhost.address, config.silkMasterPort)
        trace("client id:%s", id)
        leaderSelector.setId(id)
        leaderSelector.autoRequeue
        leaderSelector.start

        // Start a SilkClient
        val system = getActorSystem(port = config.silkClientPort)
        try {
          val dataServer: DataServer = new DataServer(config.dataServerPort)
          val t = ThreadUtil.newManager(3)
          t.submit {
            info("Starting a new DataServer(port:%d)", config.dataServerPort)
            dataServer.start
          }
          t.submit {
            val client = system.actorOf(Props(new SilkClient(leaderSelector, dataServer)), "SilkClient")
            system.awaitTermination()
          }
          t.join
        }
        finally {
          system.shutdown
          leaderSelector.close
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

  case class ClientInfo(m: MachineResource, pid: Int)
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
class SilkClient(leaderSelector:LeaderSelector, dataServer:DataServer) extends Actor with Logger {


  private var master : ActorRef = null
  private val timeout = 3 seconds

  override def preStart() = {
    info("Start SilkClient at %s:%d", localhost.address, config.silkClientPort)

    // Get an ActorRef of the SilkMaster
    try {
      val masterAddr = "akka://silk@%s/user/SilkMaster".format(leaderSelector.getLeader.getId)
      debug("Remote SilkMaster address: %s", masterAddr)
      master = context.actorFor(masterAddr)
    }
    catch {
      case e:Exception =>
        error(e)
        terminate
    }
  }


  override def postRestart(reason: Throwable) {
    info("Restart the SilkClient at %s", localhost)
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
      info("recieved run command at %s: cb:%s", localhost, cbid)
      if(!dataServer.containsClassBox(cbid)) {
        val future = master.ask(AskClassBoxHolder(cbid))(timeout)
        future onSuccess {
          case cbh:ClassBoxHolder =>
            val cb = ClassBox.sync(cbh.cb, cbh.holder)
            dataServer.register(cb)
        }
      }
      Remote.run(dataServer.getClassBox(cbid), r)
      sender ! OK
    }
    case Register(cb) => {
      if(!dataServer.containsClassBox(cb.id)) {
        info("Register a ClassBox %s to the local DataServer", cb.sha1sum)
        dataServer.register(cb)
        master ! RegisterClassBox(cb, ClientAddr(localhost, config.dataServerPort))
      }
      sender ! OK
    }
    case message => {
      warn("unknown message recieved: %s", message)
      sender ! "hello"
    }
  }

  private def terminate {
    context.stop(self)
    dataServer.stop
    context.system.shutdown()
  }

  override def postStop() {
    info("Stopped SilkClient")
  }

}


