//--------------------------------------
//
// RemoteCommandTest.scala
// Since: 2013/04/11 1:50 PM
//
//--------------------------------------

package xerial.silk.multijvm

import xerial.silk.core.{SilkInMemory, Silk}
import xerial.silk._
import xerial.core.log.Logger
import xerial.silk.cluster.SilkClient.ClientInfo
import java.net.URL
import xerial.core.io.IOUtil

object A {
  val f1 : Function1[Int, Float] = (i:Int) => i.toFloat

  def F1(i:Int) = i.toFloat

}

/**
 * @author Taro L. Saito
 */
class RemoteCommandTestMultiJvm1 extends Cluster2Spec {

  "start" in {
    start { client =>
      var v = 1024
      for(h <- Silk.hosts) {
        at(h) {
          println(s"hello $v")
        }
        v += 1
      }
      Thread.sleep(3000)
    }
  }
}

class RemoteCommandTestMultiJvm2 extends Cluster2Spec {
  "start" in {
    start { client =>  }
  }
}
