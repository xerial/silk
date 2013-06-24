package xerial.silk.cluster.framework

import xerial.silk.framework.{SilkFramework, LifeCycle}
import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import xerial.silk.util.ThreadUtil.ThreadManager
import xerial.core.log.Logger
import java.util.concurrent.{TimeUnit, Executors}
import xerial.silk.util.ThreadUtil

object ActorService extends Logger {

  val AKKA_PROTOCOL = "akka"

  private[silk] def getActorSystem(host: String = xerial.silk.cluster.localhost.address, port: Int) = {
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

  def apply(address:String, port:Int) = new ActorService {
    val actorSystem = ActorService.getActorSystem(address, port)
  }

}

trait ServiceGuard[Service] {

  def close: Unit

  def service : Service

  private def wrap[R](f: Service => R) : R = {
    try {
      f(service)
    }
    finally
      close
  }

  def map[B](f: Service => B) : B = wrap(f)
  def flatMap[B](f:Service => Option[B]) : Option[B] = wrap(f)
  def foreach[U](f:Service=>U) { wrap(f) }

}

/**
 * @author Taro L. Saito
 */
trait ActorService extends ServiceGuard[ActorSystem] with Logger {

  val actorSystem : ActorSystem

  def service = actorSystem

  def close : Unit = {
    debug(s"shut down the actor system: $actorSystem")
    actorSystem.shutdown
  }
}
