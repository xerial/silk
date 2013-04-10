//--------------------------------------
//
// ClusterTest.scala
// Since: 2013/04/10 22:52
//
//--------------------------------------

package xerial.silk.cluster

import xerial.silk.util.SilkSpec
import xerial.larray.{MMapMode, LArray}
import java.io.File
import xerial.core.io.IOUtil
import xerial.silk.util.ThreadUtil.ThreadManager
import xerial.silk.core.Silk
import xerial.silk.cluster.SilkClient.Terminate


trait ClusterSpec extends SilkSpec with ProcessBarrier {

  def numProcesses = 3
  def processID = {
    val n = getClass.getSimpleName
    val p = "[0-9]".r
    val id = p.findAllIn(n).toSeq.last.toInt
    id
  }

  before {
    xerial.silk.configureLog4j
    if(processID == 1)
      cleanup
    enterBarrier("cleanup")
  }


  def writeZkClientPort {
    if(processID == 1) {
      info(s"Write zkClientPort: ${config.zk.clientPort}")
      val m = LArray.mmap(new File("target/zkPort"), 0, 4, MMapMode.READ_WRITE)
      m.putInt(0, config.zk.clientPort)
      m.flush
      m.close
    }
  }

  def getZkConnectAddress = {
    val m = LArray.mmap(new File("target/zkPort"), 0, 4, MMapMode.READ_ONLY)
    val addr = s"127.0.0.1:${m.getInt(0)}"
    m.close
    addr
  }

  def start[U](f: => U) {
    if(processID == 1) {
      StandaloneCluster.withCluster {
        writeZkClientPort
        enterBarrier("ready")

        enterBarrier("clientStart")
        Thread.sleep(2000)
        f
      }
      enterBarrier("terminate")
    }
    else {
      enterBarrier("ready")
      withConfig(Config(silkClientPort=IOUtil.randomPort)) {
        val t = new ThreadManager(1)
        t.submit {
          SilkClient.startClient(Host(s"jvm${processID}", "127.0.0.1"), getZkConnectAddress)
        }
        enterBarrier("clientStart")
        f
        t.join
        enterBarrier("terminate")
      }
    }

  }
}

import xerial.silk

class ClusterTestMultiJvm1 extends ClusterSpec {

  "start cluster" in {
    start {
      val nodeList = Silk.hosts
      info(s"nodes: ${nodeList.mkString(", ")}")

      // do something here
      Thread.sleep(3000)
    }
  }

}

class ClusterTestMultiJvm2 extends ClusterSpec {
  "start cluster" in {
    start {}
  }

}

class ClusterTestMultiJvm3 extends ClusterSpec {
  "start cluster" in {
    start {}
  }

}
