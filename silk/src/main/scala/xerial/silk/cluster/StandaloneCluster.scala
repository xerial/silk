/*
 * Copyright 2012 Taro L. Saito
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
import xerial.silk.cluster.SilkClient.{Terminate, ClientInfo}
import xerial.core.util.Shell

object StandaloneCluster {

  def withCluster(f: => Unit) {
    val tmpDir : File = File.createTempFile("silk-tmp", "", new File("target"))
    tmpDir.mkdirs()

    var cluster : Option[StandaloneCluster] = None
    try {
      withConfig(Config(silkHome=tmpDir)) {
        cluster = Some(new StandaloneCluster)
      }
    }
    finally {
      tmpDir.rmdirs
      cluster.map(_.stop)
    }

  }


}


/**
 * Emulates the cluster environment in a single machine
 *
 * @author Taro L. Saito
 */
class StandaloneCluster extends Logger {

  private val t = ThreadUtil.newManager(2)

  private val zkHosts = Seq(ZkEnsembleHost("localhost"))
  private var zkServer : Option[ZkStandalone] = None

  // Startup a single zookeeper
  t.submit {
    val quorumConfig = new ZooKeeper(config.zk).buildQuorumConfig(0, zkHosts)
    zkServer = Some(new ZkStandalone)
    zkServer.get.run(quorumConfig)
  }

  // Access to the zookeeper, then register a SilkClient
  t.submit {
    ZooKeeper.withZkClient(zkHosts) { zkCli =>
      val jvmPID = Shell.getProcessIDOfCurrentJVM
      val m = MachineResource.thisMachine
      ClusterCommand.setClientInfo(zkCli, "localhost", ClientInfo(m, jvmPID))
      info("Start SilkClient on machine %s", m)
      SilkClient.startClient
    }
  }

  // Access to the zookeeper, then retrieve a SilkClient list (hostname and client port)


  /**
   * Terminate the standalone cluster
   */
  def stop {
    SilkClient.withLocalClient { cli =>
      cli ! Terminate
    }
    t.join
    zkServer.map(_.shutdown)
  }


}