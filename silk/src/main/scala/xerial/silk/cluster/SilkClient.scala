//--------------------------------------
//
// SilkClient.scala
// Since: 2012/12/13 4:50 PM
//
//--------------------------------------

package xerial.silk.cluster

import com.typesafe.config.ConfigFactory
import akka.actor.{Props, Actor, ActorSystem}
import xerial.core.log.Logger

object SilkClient extends Logger {

  def getActorSystem(host:String="127.0.0.1", port:Int=2552)= {

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

  lazy val system = {
    info("Starting a new ActorSystem")
    getActorSystem()
  }
  lazy val connSystem = {
    info("Starting an ActorSystem for connection")
    getActorSystem(port = 2553)
  }

  def startClient = {
    val client = system.actorOf(Props(new SilkClient), "SilkClient")
    system.awaitTermination()
  }

  def getClientAt(host:String) = {
    val akkaAddr = "akka://silk@%s:2552/user/SilkClient".format(host)
    info("remote akka actor address: %s", akkaAddr)
    connSystem.actorFor(akkaAddr)
  }

  sealed trait ClientCommand
  case class Terminate() extends ClientCommand

}


/**
 * @author Taro L. Saito
 */
class SilkClient extends Actor with Logger {

  import SilkClient._

  override def preStart() = {
    info("client started at %s", MachineResource.thisMachine.host)
  }

  protected def receive = {
    case Terminate => {
      info("Recieved terminate signal")
      sender ! "ack"
      context.stop(self)
      SilkClient.system.shutdown()
    }
    case message => {
      info("message recieved: %s", message)
      sender ! "hello"
    }
  }
  override def postStop() {
    info("client stopped.")
  }

}