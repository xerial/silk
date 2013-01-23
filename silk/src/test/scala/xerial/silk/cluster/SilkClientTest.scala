//--------------------------------------
//
// SilkClientTest.scala
// Since: 2012/12/13 5:38 PM
//
//--------------------------------------

package xerial.silk.cluster

import xerial.silk.util.SilkSpec

import xerial.silk.cluster.SilkClient.Terminate
import akka.actor.Kill
import akka.pattern.{AskTimeoutException, ask}
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent.Await
import java.util.concurrent.{Executors, TimeoutException}

/**
 * @author Taro L. Saito
 */
class SilkClientTest extends SilkSpec {

  "SilkClient" should {


    "start an actor" in {
      val t = Executors.newFixedThreadPool(5)

      implicit val timeout = 5 seconds


      t.submit(new Runnable {
        def run {
          debug("start SilkClient")
          val r = SilkClient.startClient(localhost, config.zk.zkServersConnectString)
        }
      })

      val f = t.submit(new Runnable {
        def run {
          debug("Looking up remote client")
          for(client <- SilkClient.remoteClient(Host("127.0.0.1"), config.silkClientPort)) {
            var toContinue = true
            while(toContinue) {
              try {
                debug("send message")
                val rep = client ? "hello silk!"
                debug("reply from client: %s", rep)
                toContinue = false
              }
              catch {
                case e: TimeoutException => warn(e.getMessage)
              }
            }
            debug("send termination singal")
            val v = client ? Terminate
            debug("termination reply: %s", v)
            //client ! Kill
            "ret"
          }
        } 
      })

      f.get()
    }
  }
}