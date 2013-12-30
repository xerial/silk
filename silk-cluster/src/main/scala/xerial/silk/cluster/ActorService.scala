package xerial.silk.cluster

import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import xerial.core.log.Logger
import xerial.silk.io.ServiceGuard
import xerial.core.io.IOUtil
import xerial.silk.Silk
import xerial.silk.framework.Host

object ActorService extends Logger {

  val AKKA_PROTOCOL = "akka"

  private[silk] def getActorSystem(host: String = SilkCluster.localhost.address, port: Int) = synchronized {
    debug(s"Creating an actor system using $host:$port")
    val akkaConfig = ConfigFactory.parseString(
      s"""
        |akka.loglevel = "ERROR"
        |akka.daemonic = on
        |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
        |akka.remote.transport = "akka.remote.netty.NettyRemoteTransport"
        |akka.remote.netty.connection-timeout = 15s
        |akka.remote.netty.hostname = "$host"
        |akka.remote.netty.port = $port
        |      """.stripMargin)


    //    /
    //|akka.event-handlers = ["akka.event.Logging$$DefaultLogger"]
   // |akka.log-config-on-start = on
    //    |akka.remote.enabled-transports = ["akka.remote.netty.tcp"]
    //    |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
    //    |akka.remote.netty.tcp.connection-timeout = 15s
    //      |akka.remote.netty.tcp.hostname c= "%s"
    //    |akka.remote.netty.tcp.port = %d


    //|akka.actor.serialize-messages = on
    //|akka.actor.serialize-creators = on

    ActorSystem("silk", akkaConfig, Thread.currentThread.getContextClassLoader)
  }

  def apply(address:String, port:Int) = new ActorService {
    protected[silk] val service = ActorService.getActorSystem(address, port)
  }

  def apply(host:Host) = new ActorService {
    protected[silk] val service = ActorService.getActorSystem(host.address, IOUtil.randomPort)
  }

  def local = new ActorService {
    protected[silk] val service = ActorSystem("silk-local")
  }

}


trait ActorService extends ServiceGuard[ActorSystem] with Logger {

  def close : Unit = {
    trace(s"shut down the actor system: $service")
    service.shutdown
    service.awaitTermination()
  }
}
