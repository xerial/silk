//--------------------------------------
//
// ActorServiceTest.scala
// Since: 2013/06/12 17:59
//
//--------------------------------------

package xerial.silk.cluster.framework

import xerial.silk.util.SilkSpec
import xerial.core.io.IOUtil
import xerial.core.log.Logger
import akka.actor.{Props, Actor}

object ActorServiceTest extends Logger {
  class SampleActor extends Actor {
    def receive = {
      case "Hello" => info("hello actor")
      case other => warn(s"unknown message: $other")
    }
  }

}

/**
 * @author Taro L. Saito
 */
class ActorServiceTest extends SilkSpec {
  "ActorService" should {
    "be used in for-comprehension" in {
      for(actorService <- ActorService("localhost", IOUtil.randomPort)) {
        val actor = actorService.actorOf(Props(classOf[ActorServiceTest.SampleActor]))
        actor ! "Hello"
      }
    }
  }
}