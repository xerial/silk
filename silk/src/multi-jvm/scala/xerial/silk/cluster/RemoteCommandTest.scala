//--------------------------------------
//
// RemoteCommandTest.scala
// Since: 2013/04/11 1:50 PM
//
//--------------------------------------

package xerial.silk.cluster

import xerial.silk.core.Silk
import xerial.silk._
import xerial.core.log.Logger


/**
 * @author Taro L. Saito
 */
class RemoteCommandTestMultiJvm1 extends Cluster2Spec {

  "start" in {
    start {

      for(h <- Silk.hosts) {
        debug(s"Send command to $h")
        at(h) {
          warn("hello world")
        }
      }

      Thread.sleep(3000)
    }
  }
}

class RemoteCommandTestMultiJvm2 extends Cluster2Spec {
  "start" in {
    start {
    }
  }
}
