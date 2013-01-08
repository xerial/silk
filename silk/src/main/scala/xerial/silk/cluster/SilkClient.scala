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
import xerial.lens.cui.{command, DefaultCommand}
import xerial.silk.cluster.SilkClient.Terminate
import xerial.silk.DefaultMessage
import xerial.core.io.{RichInputStream, IOUtil}
import java.net.URL
import java.io.{FileInputStream, ByteArrayOutputStream, File}
import xerial.silk.util.ThreadUtil
import org.jboss.netty.channel.socket.ServerSocketChannel
import org.jboss.netty.channel.Channel
import java.util.UUID
import com.netflix.curator.framework.recipes.leader.{LeaderSelectorListener, LeaderSelector}
import com.netflix.curator.framework.CuratorFramework
import com.netflix.curator.framework.state.ConnectionState

/**
 * SilkClient is a network interface that accepts command from the other hosts
 */
object SilkClient extends Logger {

  def getActorSystem(host: String = localhost.address, port: Int) = {

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
    debug("Starting a new ActorSystem")
    ZooKeeper.withZkClient {
      zk =>
        // Select a master among multiple clients
        val leaderSelector = new LeaderSelector(zk, config.zk.leaderElectionPath, new LeaderSelectorListener {
          private var masterSystem : Option[ActorSystem] = None
          def stateChanged(client: CuratorFramework, newState: ConnectionState) {
            if(newState == ConnectionState.LOST || newState == ConnectionState.SUSPENDED)
              shutdownMaster
          }
          def takeLeadership(client: CuratorFramework) {
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
            masterSystem map (_.shutdown)
            masterSystem = None
          }
        })

        // Start the leader selector
        val id = "%s:%s".format(localhost.address, config.silkClientPort)
        leaderSelector.setId(id)
        leaderSelector.start
        // Start a SilkClient
        val system = getActorSystem(port = config.silkClientPort)
        try {
          val dataServer: DataServer = new DataServer(config.dataServerPort)
          val t = ThreadUtil.newManager(2)
          t.submit {
            val client = system.actorOf(Props(new SilkClient(leaderSelector, dataServer)), "SilkClient")
            system.awaitTermination()
          }
          t.submit {
            info("Starting a new DataServer(port:%d)", config.dataServerPort)
            dataServer.start
          }
          t.join
        }
        finally {
          system.shutdown
          leaderSelector.close
        }

    }
  }

  def withLocalClient[U](f: ActorRef => U): U = withRemoteClient(localhost.address)(f)

  def withRemoteClient[U](host: String, clientPort: Int = config.silkClientPort)(f: ActorRef => U): U = {
    debug("Starting an ActorSystem for connection")
    val connSystem = getActorSystem(port = IOUtil.randomPort)
    try {
      val akkaAddr = "akka://silk@%s:%s/user/SilkClient".format(host, clientPort)
      debug("remote akka actor address: %s", akkaAddr)
      val actor = connSystem.actorFor(akkaAddr)
      f(actor)
    }
    finally {
      connSystem.shutdown
    }
  }


  sealed trait ClientCommand
  case object Terminate extends ClientCommand
  case object Status extends ClientCommand

  case class ClientInfo(m: MachineResource, pid: Int)
  case class Run(classBoxUUID: UUID, closure: Array[Byte])
  case class Register(cb: ClassBox)

  case object OK

}


/**
 *
 *
 *
 * @author Taro L. Saito
 */
class SilkClient(leaderSelector:LeaderSelector, dataServer:DataServer) extends Actor with Logger {

  import SilkClient._

  private var master : Option[ActorRef] = None

  override def preStart() = {
    info("SilkClient started at %s", localhost)

    // Get an ActorRef of the SilkMaster
    try {
      val masterAddr = "akka://silk@%s/user/SilkMaster".format(leaderSelector.getLeader.getId)
      master = Some(context.actorFor(masterAddr))
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
      debug("Recieved terminate signal")
      sender ! OK

      terminate
    }
    case Status => {
      info("Recieved status ping")
      sender ! OK
    }
    case r@Run(uuid, closure) => {
      info("recieved run command at %s: cb:%s", localhost, uuid)
      if(!dataServer.containsClassBox(uuid)) {


      }
      Remote.run(r)
      sender ! OK
    }
    case Register(cb) => {
      info("register ClassBox: %s", cb.sha1sum)
      dataServer.register(cb)
      sender ! OK
      info("ClassBox is registered.")
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


