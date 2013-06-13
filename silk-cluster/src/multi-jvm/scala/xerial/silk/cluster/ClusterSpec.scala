//--------------------------------------
//
// ClusterSpec.scala
// Since: 2013/04/10 22:52
//
//--------------------------------------

package xerial.silk.cluster

import xerial.silk.util.SilkSpec
import xerial.larray.{MMapMode, LArray}
import java.io.File
import xerial.core.io.IOUtil
import xerial.silk.cluster.SilkClient.{SilkClientRef, Terminate}
import xerial.silk.cluster._
import org.apache.zookeeper.{WatchedEvent, Watcher}
import org.apache.zookeeper.Watcher.Event.EventType
import com.netflix.curator.framework.recipes.barriers.DistributedDoubleBarrier
import java.util.concurrent.TimeUnit
import xerial.silk.cluster


case class Env(client:SilkClient, clientActor:SilkClientRef, zk:ZooKeeperClient)

/**
 * Base trait for testing with 4-cluster nodes
 */
trait Cluster4Spec extends ClusterSpec {
  def numProcesses = 4
}

/**
 * Base trait for testing with 3-cluster nodes
 */
trait Cluster3Spec extends ClusterSpec {
  def numProcesses = 3
}
/**
 * Base trait for testing with 2-cluster nodes
 */
trait Cluster2Spec extends ClusterSpec {
  def numProcesses = 2
}

trait ClusterSpec extends SilkSpec with ProcessBarrier {

  private val barrierPath = s"/target/lock/${this.getClass.getSimpleName.replaceAll("[0-9]+", "")}"

  def processID = {
    val n = getClass.getSimpleName
    val p = "[0-9]".r
    val id = p.findAllIn(n).toSeq.last.toInt
    id
  }

  before {
    xerial.silk.cluster.configureLog4j
    if (processID == 1) {
      cleanup
    }
    else
      Thread.sleep(1000)
  }


  def writeZkClientPort {
    if (processID == 1) {
      trace(s"Write zkClientPort: ${config.zk.clientPort}")
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



  def enterCuratorBarrier(zk: ZooKeeperClient, nodeName: String)
  {
    val cbTimeoutSec = 20 // 20 seconds
    trace(s"entering barrier: ${nodeName}")
    val ddb = new DistributedDoubleBarrier(zk.curatorFramework, s"$barrierPath/$nodeName", numProcesses)
    ddb.enter(cbTimeoutSec, TimeUnit.SECONDS)
    trace(s"exit barrier: ${nodeName}")
  }

  private var zkClient : ZooKeeperClient = null

  def enterBarrier(name:String) {
    require(zkClient != null, "must be used after zk client connection is established")
    enterCuratorBarrier(zkClient, name)
  }


  def start[U](f: Env => U) {
    try {
      if (processID == 1) {
        StandaloneCluster.withCluster {
          writeZkClientPort
          enterProcessBarrier("zkPortIsReady")
          for (zk <- ZooKeeper.zkClient(getZkConnectAddress))
          {
            zkClient = zk
            enterBarrier("zkIsReady")
            SilkClient.startClient(Host(s"jvm${processID}", "127.0.0.1"), getZkConnectAddress) {
              client =>
                enterBarrier("clientIsReady")
                try
                  f(Env(SilkClient.client.get, client, zk))
                finally
                  enterBarrier("clientBeforeFinished")
            }
            enterBarrier("clientTerminated")
          }
        }
      }
      else {
        enterProcessBarrier("zkPortIsReady") // Wait until zk port is written to a file
        for (zk <- ZooKeeper.zkClient(getZkConnectAddress))
        {
          zkClient = zk
          enterBarrier("zkIsReady")
          withConfig(Config(silkClientPort = IOUtil.randomPort, dataServerPort = IOUtil.randomPort)) {

            SilkClient.startClient(Host(s"jvm${processID}", "127.0.0.1"), getZkConnectAddress) {
              client =>
                enterBarrier("clientIsReady")
                try
                  f(Env(SilkClient.client.get, client, zk))
                finally
                  enterBarrier("clientBeforeFinished")
            }
            enterBarrier("clientTerminated")
          }
        }
      }
    }
    finally {
      //enterBarrier("terminate")
    }

  }
}


