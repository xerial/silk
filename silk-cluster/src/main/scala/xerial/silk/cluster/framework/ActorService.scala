package xerial.silk.cluster.framework

import xerial.silk.framework.{Host, SilkFramework, LifeCycle}
import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import xerial.silk.util.ThreadUtil.ThreadManager
import xerial.core.log.Logger
import java.util.concurrent.{TimeUnit, Executors}
import xerial.silk.util.ThreadUtil
import xerial.silk.io.ServiceGuard
import xerial.core.io.IOUtil

object ActorService extends Logger {

  val AKKA_PROTOCOL = "akka"

  private[silk] def getActorSystem(host: String = xerial.silk.cluster.localhost.address, port: Int) = {
    trace(s"Creating an actor system using $host:$port")
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
    protected[silk] val service = ActorService.getActorSystem(address, port)
  }

  def apply(host:Host) = new ActorService {
    protected[silk] val service = ActorService.getActorSystem(host.address, IOUtil.randomPort)
  }

}


trait ActorService extends ServiceGuard[ActorSystem] with Logger {

  def close : Unit = {
    trace(s"shut down the actor system: $service")
    service.shutdown
    service.awaitTermination()
  }
}
