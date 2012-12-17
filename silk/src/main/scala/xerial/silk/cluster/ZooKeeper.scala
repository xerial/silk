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
import org.apache.zookeeper.server.quorum.{QuorumPeerMain, QuorumPeerConfig}
import xerial.silk
import java.io.File
import xerial.core.log.Logger
import com.netflix.curator.CuratorZookeeperClient
import com.netflix.curator.retry.ExponentialBackoffRetry
import io.Source
import xerial.silk.DefaultMessage
import org.apache.log4j.BasicConfigurator
import xerial.lens.cui.{argument, option, command}
import xerial.core.util.Shell

/**
 * Interface to access ZooKeeper
 *
 * @author leo
 */
object ZooKeeper extends Logger {


  class ZkEnsembleHost(val hostName: String, val quorumPort: Int = 2888, val leaderElectionPort: Int = 3888) {
    override def toString = name
    def serverName = "%s:%s".format(hostName, quorumPort)
    def name = "%s:%s:%s".format(hostName, quorumPort, leaderElectionPort)
  }

  object ZkEnsembleHost {
    def apply(s:String) : ZkEnsembleHost = {
      val c = s.split(":")
      c.length match {
        case 2 => // host:(quorum port)
          new ZkEnsembleHost(c(0), c(1).toInt)
        case 3 => // host:(quorum port):(leader election port)
          new ZkEnsembleHost(c(0), c(1).toInt, c(2).toInt)
        case _ => // hostname only
          new ZkEnsembleHost(s)
      }
    }

    def unapply(s: String): Option[ZkEnsembleHost] = {
      try
        Some(apply(s))
      catch {
        case e => None
      }
    }
  }


  def readHostsFile(fileName:String) : Option[Seq[ZkEnsembleHost]] = {
    if (!new File(fileName).exists()) {
      warn("file %s not found", fileName)
      None
    }
    else {
      info("Reading %s", fileName)
      val r = for {
        (l, i) <- Source.fromFile(fileName).getLines().toSeq.zipWithIndex
        h <- l.trim match {
          case ZkEnsembleHost(z) => Some(z)
          case _ =>
            warn("invalid line (%d) in %s: %s", i + 1, fileName, l)
            None
        }
      }
      yield h
      val hosts = r.toSeq
      if(hosts.length == 0)
        None
      else
        Some(hosts)
    }
  }



  def buildQuorumConfig(zkHosts:Seq[ZkEnsembleHost]) : Seq[QuorumPeerConfig] = {

    val isCluster = zkHosts.length > 1

    for((zkHost, id) <- zkHosts.zipWithIndex) yield {
      val properties: Properties = new Properties
      properties.setProperty("tickTime", "2000")
      properties.setProperty("initLimit", "10")
      properties.setProperty("syncLimit", "5")
      val dataDir = new File(silk.silkHome, "log/zk/server.%d".format(id))
      if(!dataDir.exists)
        dataDir.mkdirs()
      properties.setProperty("dataDir", dataDir.getCanonicalPath)
      properties.setProperty("clientPort", "%d".format(zkHost.quorumPort))
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

  def checkZooKeeperServers(servers:Seq[ZkEnsembleHost]) : Boolean = {
    val cs = servers.map(_.serverName).mkString(",")
    // Try to connect the ZooKeeper ensemble using a short delay
    val client = new CuratorZookeeperClient(cs, 600, 150, null, new ExponentialBackoffRetry(10, 2))
    try
    {
      client.start
      client.blockUntilConnectedOrTimedOut()
    }
    catch {
      case e:Exception =>
        warn(e.getMessage)
        false
    }
    finally {
      client.close
    }
  }

  def startZooKeeperServers(servers:Seq[ZkEnsembleHost]) {

    // create config
    val quorumConfig = ZooKeeper.buildQuorumConfig(defaultZKServers)


    //
    for(s <- servers) yield {
      // login and launch the zookeeper server
      val cmd = s.hostName match {
        case "localhost" => "silk zk start -port:%d < /dev/null > /dev/null &"
        case _ =>
          val launchCmd = "silk zk start -port:%d < /dev/null > /dev/null &".format(s.quorumPort)
          """ssh %s '$SHELL -l -c "%s"'""".format(s.serverName, launchCmd)
      }
      debug("launch command:%s", cmd)



      // TODO tell zk ensemble hosts to ZooKeeperServer




    }


  }

  def defaultZKServers : Seq[ZkEnsembleHost] = {
    val homeDir = sys.props.get("user.home") getOrElse(".")
    val silkDir = homeDir + "/.silk"

    // read zkServer lists from $HOME/.clio/zkhosts file
    val ensembleServers = readHostsFile(silkDir + "/zkhosts") getOrElse {
      info("pick up candidates of zookeeper servers from $HOME/.silk/hosts")
      val randomHosts = readHostsFile(silkDir + "/hosts") map { hosts =>
        val n = hosts.length
        if(n < 3)
          Seq.empty
        else
          hosts.take(3) // use first three hosts as zk servers
      }
      randomHosts getOrElse {
        warn("Missing $HOME/.silk/hosts file. Use localhost as a zookeeper master")
        Seq(new ZkEnsembleHost("localhost"))
      }
    }
    info("zookeeper servers: %s", ensembleServers.mkString(", "))
    ensembleServers
  }



}


class ZookeeperManager {

  val client = CuratorFrameworkFactory.builder().namespace("silk-cluster").build()


  private val config = new ServerConfig()

  private val zk = new ZooKeeperServer()



}


/**
 * @author leo
 */
class ClusterCommand extends DefaultMessage with Logger {

  import ZooKeeper._

  BasicConfigurator.configure



  def findZKServer = {

    val zkServers = defaultZKServers

    // Check zoo keeper ensemble instances
    val isRunning = checkZooKeeperServers(zkServers)
    if(!isRunning) {
      info("No zookeeper server is running. Start new servers.")
      // Start zoo keeper servers
      startZooKeeperServers(zkServers)
    }



  }


  @command(description="Start up silk cluster")
  def start {

    // Find ZK servers
    val zkServers = defaultZKServers

    val zkHostsString = zkServers.map { _.name }.mkString(" ")
    val cmd = "silk cluster zkStart %s".format(zkHostsString)
    // login to each host, then launch zk
    for((s, i) <- zkServers.zipWithIndex) yield {
      // login and launch the zookeeper server
      val launchCmd = "%s -i %d < /dev/null > /dev/null &".format(cmd, i)
      val sshCmd = s.hostName match {
        case "localhost" => launchCmd
        case _ => """ssh %s '$SHELL -l -c "%s"'""".format(s.hostName, launchCmd)
      }
      info("launch command:%s", sshCmd)

      Shell.exec(sshCmd)
    }

    // Launch SilkClients on each host


  }

  @command(description="Shut down silk cluster")
  def stop {
    // Find ZK servers

    // Get Akka Actor Addresses of SilkClient

    // Send Termination signal


  }

  def zkStart(@option(prefix="-i", description="zkHost index to launch")
              id:Int,
              @argument zkHosts:Array[String])  {

    // Parse zkHosts
    val server = zkHosts.map(ZkEnsembleHost(_)).toSeq

    // Find ZooKeeperServer at localhost
    assert(id < zkHosts.length, "invalid zkhost id: %d".format(id))

    val zkHost : ZkEnsembleHost = server(id)
    val isRunning = checkZooKeeperServers(Seq(zkHost))
    if(!isRunning) {
      // start a new ZookeeperServer
      val config = ZooKeeper.buildQuorumConfig(server)
      val main = new QuorumPeerMain
      main.runFromConfig(config(id))
      // await termination
    }
    else {
      info("ZooKeeper is already running at %s", MachineResource.localhost)
    }
  }


}