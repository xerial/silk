//--------------------------------------
//
// RemoteTest.scala
// Since: 2012/12/21 1:55 PM
//
//--------------------------------------

package xerial.silk.cluster

import xerial.silk.util.SilkSpec
import xerial.core.log.Logger
import xerial.silk.cluster.closure.{LazyF0, ClosureSerializer}
import xerial.silk.cluster.{StandaloneCluster, Remote}
import java.util.concurrent.atomic.AtomicBoolean


object RemoteTest extends Logger {
  def f = { info("hello world!") }

}

/**
 * @author Taro L. Saito
 */
class RemoteTest extends SilkSpec {

  import xerial.silk.cluster.SilkCluster._

  "Remote" should {
    "run command" taggedAs("cmd") in {
      StandaloneCluster.withCluster { f =>
        val out = captureErr {
          Remote.run(ClosureSerializer.serializeClosure(RemoteTest.f))
        }
        out should (include ("hello world!"))
      }
    }


    "run Function0" taggedAs("f0") in {
      import StandaloneCluster._


      val m = captureOut {
        withClusterAndClient { client =>
          info("run remote command")
          at(StandaloneCluster.lh, client.config.cluster.silkClientPort){
            println("hello silk cluster")
          } (client)
          // TODO introduce future to wait at command result
          Thread.sleep(5000)
        }
      }
      m should (include ("hello silk cluster"))
    }

    "extract closure reference" in {
      var cl : Class[_] = null
      val out = captureOut {
        val l = LazyF0({ println("hello function0") })
        cl = l.functionClass
      }
      info(s"function0 class:$cl")
      out should (not include "hello function0")
    }

    "serialize a closure without evaluating it " taggedAs("closure") in {
      val out = captureOut {
        ClosureSerializer.serializeClosure({ println("hello function0") })
      }
      out should (not include "hello function0")
    }



  }
}