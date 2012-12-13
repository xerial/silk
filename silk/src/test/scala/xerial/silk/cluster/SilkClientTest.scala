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


/**
 * @author Taro L. Saito
 */
class SilkClientTest extends SilkSpec {

  "SilkClient" should {
    "start an actor" in {
      val t = Executors.newFixedThreadPool(5)

      val future = t.submit(new Runnable {
        def run {
          info("start SilkClient")
          SilkClient.startClient
        }
      })

      t.submit(new Runnable {
        def run {
          future.get()
          info("Looking up remote client")
          val (system, client) = SilkClient.getClientAt("127.0.0.1")
          info("send message")
          client ! "hello silk!"
          info("send termination singal")
          client ! Terminate
        }
      })


      var count = 0
      while(count < 5 && !t.awaitTermination(1, TimeUnit.SECONDS)) {
        count += 1
      }
      t.shutdown
    }
  }
}