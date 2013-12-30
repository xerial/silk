//--------------------------------------
//
// LocalActorService.scala
// Since: 2013/11/15 0:58
//
//--------------------------------------

package xerial.silk.framework

import akka.actor.ActorSystem
import xerial.silk.io.ServiceGuard
import xerial.core.log.Logger

/**
 * @author Taro L. Saito
 */
object LocalActorService {
  def local = new LocalActorService {
    protected[silk] val service = ActorSystem("silk-local")
  }
}


trait LocalActorService extends ServiceGuard[ActorSystem] with Logger {

  def close : Unit = {
    trace(s"shut down the actor system: $service")
    service.shutdown
    service.awaitTermination()
  }
}


trait LocalActorServiceComponent extends LifeCycle {

  lazy val localActorService : ActorSystem = ActorSystem("silk-local")


  override def teardown = {
    super.teardown
    localActorService.shutdown()
    localActorService.awaitTermination()
  }
}