//--------------------------------------
//
// ExecutorTest.scala
// Since: 2013/06/11 18:46
//
//--------------------------------------

package xerial.silk.cluster

import xerial.silk.framework.InMemorySliceExecutor
import xerial.core.log.{LogLevel, LoggerFactory}
import xerial.silk.cluster.framework.{RunLogger, MultiNodeExecutor}

class SliceFramework extends InMemorySliceExecutor with RunLogger


object ExecutorTest {
  def task1 = "Executor should distribute jobs"

}

import ExecutorTest._

/**
 * @author Taro L. Saito
 */
class ExecutorTestMultiJvm1 extends Cluster2Spec {
  task1 in {
    start { env =>
      val f = new SliceFramework
      val session = f.newSession


      val in = f.newSilk(Seq(1, 2, 3, 4, 5, 6))
      val a = in.map(_ * 2)
      val b = a.filter(_ < 10)
      val c = b.reduce(_ + _)

      val result = f.run(c)
      debug(s"result: $result")
    }
  }
}

class ExecutorTestMultiJvm2 extends Cluster2Spec {
  task1 in {
    start { env =>

    }
  }
}

