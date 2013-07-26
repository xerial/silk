//--------------------------------------
//
// ClusterSpec.scala
// Since: 2013/04/10 22:52
//
//--------------------------------------

package xerial.silk.cluster

import xerial.larray.{MMapMode, LArray}
import java.io.File
import xerial.core.io.IOUtil
import SilkClient.{SilkClientRef}
import xerial.silk.cluster._
import com.netflix.curator.framework.recipes.barriers.DistributedDoubleBarrier
import java.util.concurrent.TimeUnit
import xerial.silk.framework.Host
import com.netflix.curator.framework.state.{ConnectionState, ConnectionStateListener}
import xerial.core.log.{LoggerFactory, Logger}
import xerial.silk.{Silk, SilkEnv}
import xerial.silk.weaver.{ClusterSetup, StandaloneCluster}
import xerial.silk.util.SilkSpec


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
    val cbTimeoutSec = 120 // 120 seconds
    logger.trace(s"entering barrier: ${nodeName}")

    val ddb = new DistributedDoubleBarrier(zk.curatorFramework, s"$barrierPath/$nodeName", numProcesses)
    val success = ddb.enter(cbTimeoutSec, TimeUnit.SECONDS)
    if(success)
      logger.trace(s"exit barrier: ${nodeName}")
    else
      logger.warn(s"Timed out barrier await: $nodeName")
  }

  def enterBarrier(name:String) {
    require(zkClient != null, "must be used after zk client connection is established")
    enterCuratorBarrier(zkClient, name)
  }

}


trait ClusterSpecBase extends SilkSpec with ProcessBarrier with CuratorBarrier {
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


  protected def writeZkClientPort {
    if (processID == 1) {
      trace(s"Write zkClientPort: ${config.zk.clientPort}")
      val m = LArray.mmap(new File("target/zkPort"), 0, 4, MMapMode.READ_WRITE)
      m.putInt(0, config.zk.clientPort)
      m.flush
      m.close
    }
  }

  protected def getZkConnectAddress = {
    val m = LArray.mmap(new File("target/zkPort"), 0, 4, MMapMode.READ_ONLY)
    val addr = s"127.0.0.1:${m.getInt(0)}"
    m.close
    addr
  }


  protected var zkClient : ZooKeeperClient = null

  def nodeName : String = s"jvm${processID}"

}


trait ClusterSpec extends ClusterSpecBase {


  def start[U](f: Env => U) {
    try {
      if (processID == 1) {
        StandaloneCluster.withCluster { clusterEnv =>
          zkClient = clusterEnv.zk
          writeZkClientPort
          enterProcessBarrier("zkPortIsReady")
          ClusterSetup.startClient(Host(nodeName, "127.0.0.1"), clusterEnv.zk) {
            env =>
            // Set SilkEnv global variable
              Silk.setEnv(new SilkEnvImpl(clusterEnv.zk, clusterEnv.actorSystem, SilkClient.client.get.dataServer))

              // Record the cluster state
              env.zk.set(config.zk.clusterStatePath, "started".getBytes())
              enterBarrier("clientIsReady")
              try
                f(Env(SilkClient.client.get, env.clientRef, env.zk))
              finally {
                env.zk.set(config.zk.clusterStatePath, "shutdown".getBytes())
                enterBarrier("beforeShutdown")
              }
          }
        }
      }
      else {
        enterProcessBarrier("zkPortIsReady") // Wait until zk port is written to a file
        val zkAddr = getZkConnectAddress
        withConfig(Config.testConfig(zkAddr)) {
          ClusterSetup.startClient(Host(nodeName, "127.0.0.1"), zkAddr) {
            env =>
              zkClient = env.zk
              enterBarrier("clientIsReady")
              try
                f(Env(SilkClient.client.get, env.clientRef, env.zk))
              finally
                enterBarrier("beforeShutdown")
          }
        }
      }
    }
    finally {
      //enterBarrier("terminate")
    }

  }

}

trait ClusterUserSpec extends ClusterSpecBase {

  def start[U](f: String => U) {
    enterProcessBarrier("zkPortIsReady") // Wait until zk port is written to a file
    val zkAddr = getZkConnectAddress
    val zk = ZooKeeper.zkClient(zkAddr)
    zkClient = zk.service
    enterBarrier("clientIsReady")
    try
      f(zkAddr)
    finally
      enterBarrier("beforeShutdown")
  }

}


trait ClusterUser4Spec extends ClusterUserSpec {

  def numProcesses = 4

}

trait ClusterUser3Spec extends ClusterUserSpec {

  def numProcesses = 3

}

trait ClusterUser2Spec extends ClusterUserSpec {

  def numProcesses = 2

}


