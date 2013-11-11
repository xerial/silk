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
import xerial.silk.util.{Log4jUtil, ProcessBarrier, SilkSpec}
import xerial.silk.framework.SilkClient.SilkClientRef
import xerial.silk._

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



  def start[U](f: SilkEnvImpl => U) {
    try {
      if (processID == 1) {
        StandaloneCluster.withCluster {
          writeZkClientPort
          enterProcessBarrier("zkPortIsReady")
          ClusterSetup.startClient(Host(nodeName, "127.0.0.1"), config.zk.zkServersConnectString) {
            env =>
              val e = env.asInstanceOf[SilkEnvImpl]
              zkClient = e.zk
              // Record the cluster state
              e.zk.set(config.zk.clusterStatePath, "started".getBytes())
              enterBarrier("clientIsReady")
              try
                f(e)
              finally {
                enterBarrier("beforeShutdown")
                e.zk.set(config.zk.clusterStatePath, "shutdown".getBytes())
              }
          }
        }
      }
      else {
        enterProcessBarrier("zkPortIsReady") // Wait until zk port is written to a file
        val zkAddr = getZkConnectAddress
        var tmpDir : Option[File] = None
        try {
          withConfig(Config.testConfig(zkAddr)) {
            tmpDir = Some(config.silkHome)
            ClusterSetup.startClient(Host(nodeName, "127.0.0.1"), zkAddr) {
              env =>
                val e = env.asInstanceOf[SilkEnvImpl]
                zkClient = e.zk
                enterBarrier("clientIsReady")
                try
                  f(e)
                finally
                  enterBarrier("beforeShutdown")
            }
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


trait Cluster4UserSpec extends ClusterUserSpec {

  def numProcesses = 4

}

trait Cluster3UserSpec extends ClusterUserSpec {

  def numProcesses = 3

}

trait Cluster2UserSpec extends ClusterUserSpec {

  def numProcesses = 2

}


