//--------------------------------------
//
// SilkClientTest.scala
// Since: 2012/12/13 5:38 PM
//
//--------------------------------------

package xerial.silk.cluster

import xerial.silk.util.SilkSpec
import actors.threadpool.{TimeUnit, Executors}
import xerial.silk.cluster.SilkClient.Terminate
import akka.actor.Kill
import akka.pattern.{AskTimeoutException, ask}
import akka.util.Timeout
import akka.util.duration._
import akka.dispatch.Await
import java.util.concurrent.TimeoutException

/**
 * @author Taro L. Saito
 */
class SilkClientTest extends SilkSpec {

  "SilkClient" should {

    after {
      SilkClient.system.shutdown()
      SilkClient.connSystem.shutdown()
    }


    "start an actor" in {
      val t = Executors.newFixedThreadPool(5)

      implicit val timeout = Timeout(5 seconds)


      t.submit(new Runnable {
        def run {
          info("start SilkClient")
          val r = SilkClient.startClient
        }
      })

      val f = t.submit(new Runnable {
        def run {
          info("Looking up remote client")
          val client = SilkClient.getClientAt("127.0.0.1")
          var toContinue = true
          while(toContinue) {
            try {
              debug("send message")
              val f = (client ? "hello silk!").mapTo[String]
              val rep = Await.result(f, timeout.duration)
              debug("reply from client: %s", rep)
              toContinue = false
            }
            catch {
              case e: TimeoutException => warn(e.getMessage)
            }
          }
          debug("send termination singal")
          val f = client ? Terminate
          val v = f.value
          debug("termination reply: %s", v)
          //client ! Kill
          "ret"
        } 
      })

      f.get()
    }
  }
}