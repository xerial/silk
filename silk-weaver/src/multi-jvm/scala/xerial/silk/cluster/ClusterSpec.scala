//--------------------------------------
//
// ClusterSpec.scala
// Since: 2013/04/10 22:52
//
//--------------------------------------

package xerial.silk.cluster

import xerial.larray.{MMapMode, LArray}
import java.io.File
import xerial.silk.util.Path._
import xerial.silk.framework._
import SilkClient.{SilkClientRef}
import com.netflix.curator.framework.recipes.barriers.DistributedDoubleBarrier
import java.util.concurrent.TimeUnit
import xerial.core.log.LoggerFactory
import xerial.silk.weaver.{StandaloneCluster, ClusterSetup}
import xerial.silk.util.{Log4jUtil, SilkSpec}
import SilkClient.SilkClientRef
import xerial.silk.SilkException

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

  def numProcesses:Int
  protected def zkClient:ZooKeeperClient

  protected def enterCuratorBarrier(zk:ZooKeeperClient, nodeName:String) {
    val cbTimeoutSec = 120 // 120 seconds
    logger.trace(s"entering barrier: ${nodeName}")

    val ddb = new DistributedDoubleBarrier(zk.curatorFramework, s"$barrierPath/$nodeName", numProcesses)
    val success = ddb.enter(cbTimeoutSec, TimeUnit.SECONDS)
    if (success)
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

  Log4jUtil.suppressLog4jwarning

  def processID = {
    val n = getClass.getSimpleName
    val p = "[0-9]".r
    val id = p.findAllIn(n).toSeq.last.toInt
    id
  }

  before {
    if (processID == 1) {
      cleanup
    }
    else
      Thread.sleep(500)
  }


  protected def writeZkClientPort(port:Int) {
    if (processID == 1) {
      trace(s"Write zkClientPort: ${port}")
      val m = LArray.mmap(new File("target/zkPort"), 0, 4, MMapMode.READ_WRITE)
      m.putInt(0, port)
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


  protected var zkClient:ZooKeeperClient = null

  def nodeName:String = s"jvm${processID}"
}


trait ClusterSpec extends ClusterSpecBase {


  def start[U](body:SilkClientService => U) {
    try {
      if (processID == 1) {
        StandaloneCluster.withCluster {
          framework =>
            writeZkClientPort(framework.config.zk.clientPort)
            enterProcessBarrier("zkPortIsReady")
            ClusterSetup.startClient(framework.config, Host(nodeName, "127.0.0.1"), framework.zkConnectString) {
              client =>
                zkClient = client.zk
                // Record the cluster state
                client.zk.set(client.config.zk.clusterStatePath, "started".getBytes())
                enterBarrier("clientIsReady")
                try
                  body(client)
                finally {
                  enterBarrier("beforeShutdown")
                  client.zk.set(client.config.zk.clusterStatePath, "shutdown".getBytes())
                }
            }
        }
      }
      else {
        enterProcessBarrier("zkPortIsReady") // Wait until zk port is written to a file
        val zkAddr = getZkConnectAddress
        var tmpDir:Option[File] = None
        try {
          val f = SilkClusterFramework.forTest(zkAddr)
          tmpDir = Some(f.config.home.silkHome)
          ClusterSetup.startClient(f.config, Host(nodeName, "127.0.0.1"), zkAddr) {
            client =>
              zkClient = client.zk
              enterBarrier("clientIsReady")
              try
                body(client)
              finally
                enterBarrier("beforeShutdown")
          }
        }
        finally {
          tmpDir.map(_.rmdirs)
        }
      }
    }
    finally {
      //enterBarrier("terminate")
    }

  }

}

trait ClusterUserSpec extends ClusterSpecBase {

  def start[U](body:String => U) {
    enterProcessBarrier("zkPortIsReady") // Wait until zk port is written to a file
    val zkAddr = getZkConnectAddress
    val f = SilkClusterFramework.forTest(zkAddr)
    val zk = ZooKeeper.zkClient(f.config.zk, zkAddr)
    zkClient = zk.service
    enterBarrier("clientIsReady")
    try
      body(zkAddr)
    finally
      enterBarrier("beforeShutdown")
  }

}


trait Cluster4UserSpec extends ClusterUserSpec {

  def numProcesses = 4

}

trait Cluster3UserSpec extends ClusterUserSpec {

  def numProcesses = 3

}

trait Cluster2UserSpec extends ClusterUserSpec {

  def numProcesses = 2

}


