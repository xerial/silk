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



  var dataServer: Option[DataServer] = None

  def startClient = {
    debug("Starting a new ActorSystem")
    val system = getActorSystem(port = config.silkClientPort)
    try {
      val t = ThreadUtil.newManager(2)
      t.submit {
        val client = system.actorOf(Props(new SilkClient), "SilkClient")
        system.awaitTermination()
      }
      t.submit {
        info("Starting a new DataServer(port:%d)", config.dataServerPort)
        dataServer = Some(new DataServer(config.dataServerPort))
        dataServer map (_.start)
      }
      t.join
    }
    finally
      system.shutdown
  }

  def withLocalClient[U](f: ActorRef => U) : U = withRemoteClient(localhost.address)(f)

  def withRemoteClient[U](host: String, clientPort:Int = config.silkClientPort)(f: ActorRef => U) : U = {
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
  case class Terminate() extends ClientCommand
  case class Status() extends ClientCommand


  case class ClientInfo(m: MachineResource, pid: Int)

  case class Run(classBoxUUID: UUID, closure: Array[Byte])
  case class Register(cb: ClassBox)


  case class SendJar(url: URL)

  case class Jar(url: URL, binary: Array[Byte])

}


/**
 *
 *
 *
 * @author Taro L. Saito
 */
class SilkClient extends Actor with Logger {

  import SilkClient._

  override def preStart() = {
    info("SilkClient started at %s", MachineResource.thisMachine.host)
  }

  protected def receive = {
    case Terminate => {
      debug("Recieved terminate signal")
      sender ! "ack"
      context.stop(self)
      context.system.shutdown()

      // stop data server
      dataServer map (_.stop)
    }
    case Status => {
      info("Recieved status ping")
      sender ! "OK"
    }
    case r@Run(cb, closure) => {
      info("recieved run command at %s: cb:%s", localhost, cb.sha1sum)
      Remote.run(r)
      sender ! "OK"
    }
    case Register(cb) => {
      info("register ClassBox: %s", cb.sha1sum)
      dataServer.map {
        _.register(cb)
      }
      sender ! "OK"
      info("ClassBox is registered.")
    }
    case SendJar(url) => {
      val f = new File(url.getPath)
      if (f.exists) {
        // Jar file size must be up to 2GB
        val binary = new Array[Byte](f.length.toInt)
        IOUtil.withResource(new RichInputStream(new FileInputStream(f))) {
          fin =>
            fin.readFully(binary)
        }
        sender ! Jar(url, binary)
      }
    }
    case message => {
      warn("unknown message recieved: %s", message)
      sender ! "hello"
    }
  }
  override def postStop() {
    info("Stopped SilkClient")
  }

}


