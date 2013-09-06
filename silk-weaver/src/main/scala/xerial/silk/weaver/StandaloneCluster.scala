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

package xerial.silk.weaver

import java.io.File
import xerial.core.io.Path._
import xerial.core.log.Logger
import xerial.silk.cluster._
import com.netflix.curator.test.{InstanceSpec, TestingServer, TestingZooKeeperServer}
import xerial.core.io.IOUtil
import xerial.silk.framework.Host
import xerial.silk.cluster.framework.ActorService
import xerial.silk.{SilkEnv, Silk}
import xerial.silk.cluster.SilkClient.SilkClientRef
import akka.actor.ActorSystem


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
      webUIPort = IOUtil.randomPort,
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

  case class ClusterEnv(zk:ZooKeeperClient, actorSystem:ActorSystem)

  def withCluster(f: ClusterEnv => Unit) {
    var cluster : Option[StandaloneCluster] = None
    try {
      withConfig(randomConfig) {
        cluster = Some(new StandaloneCluster)
        cluster map (_.start)
        for{
          zk <- ZooKeeper.defaultZkClient
          actorSystem <- ActorService(localhost.address, IOUtil.randomPort)
        } yield {
          val e = ClusterEnv(zk, actorSystem)
          f(e)
        }
      }
    }
    finally {
      cluster.map(_.stop)
    }
  }

  def withClusterAndClient(f:SilkClientRef => Unit) {
    withCluster { clusterEnv =>
      ClusterSetup.startClient(lh, config.zk.zkServersConnectString) { env =>
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
    info(s"Start a zookeeper server: ${config.zk.zkServersConnectString}, zkDir:${config.zkDir}")
    //val quorumConfig = ZooKeeper.buildQuorumConfig(0, config.zk.getZkServers)
    zkServer = Some(new TestingServer(new InstanceSpec(config.zkDir, config.zk.clientPort, config.zk.quorumPort, config.zk.leaderElectionPort, false, 0)))
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