/*
 * Copyright 2013 Taro L. Saito
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//--------------------------------------
//
// StandaloneCluster.scala
// Since: 2012/12/29 22:33
//
//--------------------------------------

package xerial.silk.cluster

import java.io.File
import xerial.core.log.Logger
import xerial.silk.cluster._
import com.netflix.curator.test.{ByteCodeRewrite, InstanceSpec, TestingServer}
import xerial.core.io.IOUtil
import xerial.silk.framework._
import xerial.silk.util.{Log4jUtil, Path, Guard}
import java.util.concurrent.TimeUnit
import scala.Some
import xerial.silk.cluster.ZkConfig


object StandaloneCluster {


  val lh = Host("localhost", "127.0.0.1")

  import Path._


  def withCluster(body: ClusterWeaver => Unit) {
    var cluster : Option[StandaloneCluster] = None
    val tmpDir : File = IOUtil.createTempDir(new File("target"), "silk-tmp").getAbsoluteFile
    try {
      val zkp = IOUtil.randomPort

      val f = new ClusterWeaver {
        // Generate a configuration using available ports
        override lazy val zkConnectString = s"127.0.0.1:${zkp}"
        override val config = ClusterWeaverConfig(
          home = HomeConfig(silkHome=tmpDir),
          cluster = ClusterConfig(
            silkClientPort = IOUtil.randomPort,
            silkMasterPort = IOUtil.randomPort,
            dataServerPort = IOUtil.randomPort,
            dataServerKeepAlive = false,
            webUIPort = IOUtil.randomPort,
            launchWebUI = false
          ),
          zk=ZkConfig(
            zkHosts = tmpDir / "zkhosts",
            zkDir = tmpDir / "local" / "zk",
            clientPort = zkp,
            quorumPort = IOUtil.randomPort,
            leaderElectionPort = IOUtil.randomPort,
            clientConnectionMaxRetry  = 2,
            clientConnectionTimeout = 1000,
            clientConnectionTickTime = 300
          )
        )
      }
      cluster = Some(new StandaloneCluster(f))
      cluster map (_.start)
      body(f)
    }
    finally {
      cluster.map(_.stop)
      tmpDir.rmdirs
    }
  }

  def withClusterAndClient(body:SilkClientService => Unit) {
    withCluster { f =>
      ClusterSetup.startClient(f.config, lh, f.zkConnectString) { client =>
        body(client)
      }
    }
  }

  class ClusterHandle extends Guard {

    private val isShutdown = newCondition
    private var keepRunning = true
    private val t = new Thread(new Runnable {
      def run() {
        withCluster { framework =>
          guard {
            while(keepRunning) {
              isShutdown.await(1, TimeUnit.SECONDS)
            }
          }
        }
      }
    })
    t.setDaemon(true)
    t.start()

    def stop = {
      guard {
        keepRunning = false
        isShutdown.signalAll()
      }
    }
  }

  def startTestCluster = new ClusterHandle



}


/**
 * Emulates the cluster environment in a single machine
 *
 * @author Taro L. Saito
 */
class StandaloneCluster(f:ClusterWeaver) extends Logger {



  private var zkServer : Option[TestingServer] = None


  def start {
    // Startup a single zookeeper
    info(s"Start a zookeeper server: ${f.zkConnectString}, zkDir:${f.config.zk.zkDir}")
    //val quorumConfig = ZooKeeper.buildQuorumConfig(0, config.zk.getZkServers)
    val config = f.config
    zkServer = Some(new TestingServer(new InstanceSpec(config.zk.zkDir, config.zk.clientPort, config.zk.quorumPort, config.zk.leaderElectionPort, false, 0)))
    debug(s"ZooKeeper is ready")
  }

  // Access to the zookeeper, then retrieve a SilkClient list (hostname and client port)

  /**
   * Terminate the standalone cluster
   */
  def stop {
//    val H = hosts
//    info(s"Sending a terminate signal to the clients: ${H.mkString(", ")}")
//    for(h <- H.par; cli <- SilkClient.remoteClient(h.host, h.clientPort)) {
//      cli ! Terminate
//    }
    info("Stopping ZooKeeper server")
    zkServer.map(_.stop)
  }


}