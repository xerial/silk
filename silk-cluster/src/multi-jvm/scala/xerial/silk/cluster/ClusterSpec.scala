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

trait ClusterSpec extends SilkSpec with ProcessBarrier with ConnectionStateListener {

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


  private def enterCuratorBarrier(zk: ZooKeeperClient, nodeName: String)
  {
    val cbTimeoutSec = 10 // 10 seconds
    debug(s"entering barrier: ${nodeName}")

    val ddb = new DistributedDoubleBarrier(zk.curatorFramework, s"$barrierPath/$nodeName", numProcesses)
    ddb.enter(cbTimeoutSec, TimeUnit.SECONDS)
    debug(s"exit barrier: ${nodeName}")
  }

  /**
   * Called when there is a state change in the connection
   *
   * @param client the client
   * @param newState the new state
   */
  def stateChanged(client: CuratorFramework, newState: ConnectionState) {

    newState match {
      case ConnectionState.LOST =>
        warn(s"Connection lost")
      case ConnectionState.SUSPENDED =>
        warn(s"Connection suspended")
      case _ =>
    }
  }


  private var zkClient : ZooKeeperClient = null

  def enterBarrier(name:String) {
    require(zkClient != null, "must be used after zk client connection is established")
    enterCuratorBarrier(zkClient, name)
  }

  def nodeName : String = s"jvm${processID}"

  def start[U](f: Env => U) {
    try {
      if (processID == 1) {
        StandaloneCluster.withCluster {
          writeZkClientPort
          enterProcessBarrier("zkPortIsReady")
          for (zk <- ZooKeeper.zkClient(getZkConnectAddress))
          {
            zk.curatorFramework.getConnectionStateListenable.addListener(this)
            zkClient = zk

            enterBarrier("zkIsReady")
            SilkClient.startClient(Host(nodeName, "127.0.0.1"), getZkConnectAddress) {
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
        for (zk <- ZooKeeper.zkClient(getZkConnectAddress, timeOut=1000))
        {
          zk.curatorFramework.getConnectionStateListenable.addListener(this)
          zkClient = zk
          enterBarrier("zkIsReady")
          withConfig(Config(silkClientPort = IOUtil.randomPort, dataServerPort = IOUtil.randomPort)) {

            SilkClient.startClient(Host(nodeName, "127.0.0.1"), getZkConnectAddress) {
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


