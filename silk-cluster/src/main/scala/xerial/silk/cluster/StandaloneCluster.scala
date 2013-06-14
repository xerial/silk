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
import xerial.core.io.Path._
import xerial.silk.cluster.ZooKeeper.{ZkStandalone, ZkQuorumPeer}
import xerial.silk.util.ThreadUtil
import xerial.core.log.Logger
import xerial.silk.cluster.SilkClient.{SilkClientRef, RegisterClassBox, Terminate}
import xerial.core.util.Shell
import xerial.silk.cluster._
import com.netflix.curator.test.{InstanceSpec, TestingServer, TestingZooKeeperServer}
import xerial.core.io.IOUtil
import akka.pattern.{AskTimeoutException, ask}
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent.Await
import java.util.concurrent.TimeoutException
import xerial.silk.framework.Host


object StandaloneCluster {

  val lh = Host("localhost", "127.0.0.1")


  def randomConfig : Config = {
    val tmpDir : File = IOUtil.createTempDir(new File("target"), "silk-tmp").getAbsoluteFile
    val zkClientPort = IOUtil.randomPort
    val zkLeaderElectionPort = IOUtil.randomPort
    val zkQuorumPort = IOUtil.randomPort

    val config = Config(silkHome=tmpDir,
      silkClientPort = IOUtil.randomPort,
      silkMasterPort = IOUtil.randomPort,
      dataServerPort = IOUtil.randomPort,
      zk=ZkConfig(
        zkServers = Some(Seq(new ZkEnsembleHost(lh, clientPort=zkClientPort, leaderElectionPort = zkLeaderElectionPort, quorumPort = zkQuorumPort))),
        clientPort = zkClientPort,
        quorumPort = zkQuorumPort,
        leaderElectionPort = zkLeaderElectionPort,
        clientConnectionMaxRetry  = 2,
        clientConnectionTimeout = 1000,
        clientConnectionTickTime = 300
    ))

    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
      def run() {
        // delete on exit
        tmpDir.rmdirs
      }
    }))

    config
  }

  def withCluster(f: => Unit) {
    var cluster : Option[StandaloneCluster] = None
    try {
      withConfig(randomConfig) {
        cluster = Some(new StandaloneCluster)
        cluster map (_.start)
        f
      }
    }
    finally {
      cluster.map(_.stop)
    }
  }

  def withClusterAndClient(f:SilkClientRef => Unit) {
    withCluster {
      SilkClient.startClient(lh, config.zk.zkServersConnectString) { env =>
        f(env.clientRef)
      }
    }
  }


}


/**
 * Emulates the cluster environment in a single machine
 *
 * @author Taro L. Saito
 */
class StandaloneCluster extends Logger {

  suppressLog4jwarning

  private var zkServer : Option[TestingServer] = None

  import StandaloneCluster._

  def start {
    // Startup a single zookeeper
    info(s"Running a zookeeper server. zkDir:${config.zkDir}")
    //val quorumConfig = ZooKeeper.buildQuorumConfig(0, config.zk.getZkServers)
    zkServer = Some(new TestingServer(new InstanceSpec(config.zkDir, config.zk.clientPort, config.zk.quorumPort, config.zk.leaderElectionPort, false, 0)))
    debug(s"ZooKeeper is ready")
  }

  // Access to the zookeeper, then retrieve a SilkClient list (hostname and client port)

  /**
   * Terminate the standalone cluster
   */
  def stop {
    debug("Sending a stop signal to the clients")
    for(h <- hosts; cli <- SilkClient.remoteClient(h.host, h.clientPort)) {
      cli ! Terminate
    }
    debug("Shutting down the zookeeper server")
    zkServer.map(_.stop)
  }


}