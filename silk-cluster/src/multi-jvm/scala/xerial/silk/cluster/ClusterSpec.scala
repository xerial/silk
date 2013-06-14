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
import xerial.silk.cluster.SilkClient.{SilkClientRef}
import xerial.silk.cluster._
import com.netflix.curator.framework.recipes.barriers.DistributedDoubleBarrier
import java.util.concurrent.TimeUnit
import xerial.silk.framework.Host
import com.netflix.curator.framework.state.{ConnectionState, ConnectionStateListener}
import com.netflix.curator.framework.CuratorFramework
import org.apache.zookeeper.KeeperException.ConnectionLossException
import org.apache.log4j.Level
import xerial.core.log.{LoggerFactory, Logger}


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


trait CuratorBarrier {

  private val logger = LoggerFactory(classOf[CuratorBarrier])

  private val barrierPath = s"/silk/test/lock/${this.getClass.getSimpleName.replaceAll("[0-9]+", "")}"

  def numProcesses : Int
  protected def zkClient: ZooKeeperClient

  protected def enterCuratorBarrier(zk: ZooKeeperClient, nodeName: String)
  {
    val cbTimeoutSec = 20 // 20 seconds
    logger.trace(s"entering barrier: ${nodeName}")

    val ddb = new DistributedDoubleBarrier(zk.curatorFramework, s"$barrierPath/$nodeName", numProcesses)
    ddb.enter(cbTimeoutSec, TimeUnit.SECONDS)
    logger.trace(s"exit barrier: ${nodeName}")
  }

  def enterBarrier(name:String) {
    require(zkClient != null, "must be used after zk client connection is established")
    enterCuratorBarrier(zkClient, name)
  }

}


trait ClusterSpec extends SilkSpec with ProcessBarrier with CuratorBarrier {

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
      Thread.sleep(500)
  }


  private def writeZkClientPort {
    if (processID == 1) {
      trace(s"Write zkClientPort: ${config.zk.clientPort}")
      val m = LArray.mmap(new File("target/zkPort"), 0, 4, MMapMode.READ_WRITE)
      m.putInt(0, config.zk.clientPort)
      m.flush
      m.close
    }
  }

  private def getZkConnectAddress = {
    val m = LArray.mmap(new File("target/zkPort"), 0, 4, MMapMode.READ_ONLY)
    val addr = s"127.0.0.1:${m.getInt(0)}"
    m.close
    addr
  }


  protected var zkClient : ZooKeeperClient = null

  def nodeName : String = s"jvm${processID}"

  def start[U](f: Env => U) {
    try {
      if (processID == 1) {
        StandaloneCluster.withCluster {
          writeZkClientPort
          enterProcessBarrier("zkPortIsReady")
          SilkClient.startClient(Host(nodeName, "127.0.0.1"), getZkConnectAddress) {
            env =>
              zkClient = env.zk
              enterBarrier("clientIsReady")
              try
                f(Env(SilkClient.client.get, env.clientRef, env.zk))
              finally
                enterBarrier("clientBeforeTerminate")
          }
        }
      }
      else {
        enterProcessBarrier("zkPortIsReady") // Wait until zk port is written to a file
        withConfig(Config(silkClientPort = IOUtil.randomPort, dataServerPort = IOUtil.randomPort)) {
          SilkClient.startClient(Host(nodeName, "127.0.0.1"), getZkConnectAddress) {
            env =>
              zkClient = env.zk
              enterBarrier("clientIsReady")
              try
                f(Env(SilkClient.client.get, env.clientRef, env.zk))
              finally
                enterBarrier("clientBeforeTerminate")
          }
        }
      }
    }
    finally {
      //enterBarrier("terminate")
    }

  }

}


