/*
 * Copyright 2012 Taro L. Saito
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//--------------------------------------
//
// ZooKeeper.scala
// Since: 2012/10/23 6:09 PM
//
//--------------------------------------

package xerial.silk.cluster

import org.apache.zookeeper.server.{ServerConfig, ZooKeeperServer}
import com.netflix.curator.framework.{CuratorFrameworkFactory, CuratorFramework}
import java.util.Properties
import org.apache.zookeeper.server.quorum.QuorumPeerConfig
import xerial.silk
import java.io.File

/**
 * Interface to access ZooKeeper
 *
 * @author leo
 */
object ZooKeeper {




  def buildQuorumConfig(id:Int, zkHosts:Seq[ZkEnsembleHost]) : Seq[QuorumPeerConfig] = {

    val isCluster = zkHosts.length > 1

    for((zkHost, id) <- zkHosts.zipWithIndex) yield {
      val properties: Properties = new Properties
      properties.setProperty("initLimit", "10")
      properties.setProperty("syncLimit", "5")
      val dataDir = new File(silk.silkHome, "/log/zk/server.%d".format(id))
      if(!dataDir.exists)
        dataDir.mkdirs()
      properties.setProperty("dataDir", dataDir.getCanonicalPath)
      properties.setProperty("clientPort", Integer.toString(zkHost.quorumPort))
      if (isCluster) {
        for ((h, hid) <- zkHosts.zipWithIndex) {
          properties.setProperty("server." + hid, "%s:%d:%d".format(h.hostName, h.quorumPort, h.leaderElectionPort))
        }
      }
      val config: QuorumPeerConfig = new QuorumPeerConfig
      config.parseProperties(properties)
      config
    }

  }


}


class ZookeeperManager {

  val client = CuratorFrameworkFactory.builder().namespace("silk-cluster").build()


  private val config = new ServerConfig()

  private val zk = new ZooKeeperServer()



}