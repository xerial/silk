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

import org.apache.zookeeper.server.{ZooKeeperServerMain, ServerConfig, ZooKeeperServer}
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
import xerial.core.util.{CommandLineTokenizer, Shell}
import com.google.common.io.Files
import java.util.concurrent.{TimeUnit, Executors, ExecutorService}
import com.netflix.curator.utils.EnsurePath

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


  def writeMyID(id:Int) {
    val dataDir = new File(silk.silkHome, "log/zk/server.%d".format(id))
    if(!dataDir.exists)
      dataDir.mkdirs()


    val myIDFile = new File(dataDir, "myid")
    info("creating myid file at: %s", myIDFile)
    if(!myIDFile.exists()) {
      Files.write("%d".format(id).getBytes, myIDFile)
    }
  }


  def buildQuorumConfig(id:Int, zkHosts:Seq[ZkEnsembleHost]) : QuorumPeerConfig = {

    val isCluster = zkHosts.length > 1

    if(isCluster) {
      info("write myid: %d", id)
      ZooKeeper.writeMyID(id)
    }

    val zkHost = zkHosts(id)
    val properties: Properties = new Properties
    properties.setProperty("tickTime", "2000")
    properties.setProperty("initLimit", "10")
    properties.setProperty("syncLimit", "5")
    val dataDir = new File(silk.silkHome, "log/zk/server.%d".format(id))
    if(!dataDir.exists)
      dataDir.mkdirs()

    properties.setProperty("dataDir", dataDir.getCanonicalPath)
    properties.setProperty("clientPort", "2181")
    if (isCluster) {
      for ((h, hid) <- zkHosts.zipWithIndex) {
        properties.setProperty("server." + hid, "%s:%d:%d".format(h.hostName, h.quorumPort, h.leaderElectionPort))
      }
    }
    val config: QuorumPeerConfig = new QuorumPeerConfig
    config.parseProperties(properties)
    config
  }

  def isAvailable(servers:Seq[ZkEnsembleHost]) : Boolean = {
    val cs = servers.map(_.serverName).mkString(",")
    // Try to connect the ZooKeeper ensemble using a short delay
    info("Starting a zookeeper client")
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





  def defaultZKServers : Seq[ZkEnsembleHost] = {
    val homeDir = sys.props.get("user.home") getOrElse(".")
    val silkDir = homeDir + "/.silk"

    // read zkServer lists from $HOME/.clio/zkhosts file
    val ensembleServers : Seq[ZkEnsembleHost] = readHostsFile(silkDir + "/zkhosts") getOrElse {
      info("pick up candidates of zookeeper servers from $HOME/.silk/hosts")
      val randomHosts = readHostsFile(silkDir + "/hosts") filter { hosts => hosts.length >= 3} map { hosts =>
        hosts.take(3) // use first three hosts as zk servers
      }
      randomHosts.getOrElse {
        warn("Not enough servers found in $HOME/.silk/hosts file (required more than 3 servers). Use localhost as a single zookeeper master")
        Seq(new ZkEnsembleHost(MachineResource.localhost.address))
      }
    }

    info("zookeeper servers: %s", ensembleServers.mkString(", "))
    ensembleServers
  }


  private[cluster] trait ZkServer {
    def run(config:QuorumPeerConfig) : Unit
    def shutdown : Unit
  }

  private[cluster] class ZkQuorumPeer extends QuorumPeerMain with ZkServer {
    def run(config:QuorumPeerConfig) : Unit = {
      runFromConfig(config)
    }
    def shutdown {
      quorumPeer.shutdown
    }
  }

  private[cluster] class ZkStandalone extends ZooKeeperServerMain with ZkServer {
    def run(config:QuorumPeerConfig) : Unit = {
      val sConfig = new ServerConfig
      sConfig.readFrom(config)
      runFromConfig(sConfig)
    }

    override def shutdown {
      super.shutdown

    }

  }


}

/**
 * @author leo
 */
class ClusterCommand extends DefaultMessage with Logger {

  import ZooKeeper._

  BasicConfigurator.configure
  org.apache.log4j.Logger.getRootLogger.setLevel(org.apache.log4j.Level.WARN)


  @command(description="Start up silk cluster")
  def start {

    // Find ZK servers
    val zkServers = defaultZKServers

    val zkHostsString = zkServers.map { _.name }.mkString(" ")
    val cmd = "silk cluster zkStart %s".format(zkHostsString)
    // login to each host, then launch zk
    for((s, i) <- zkServers.zipWithIndex) {
      // login and launch the zookeeper server
      val launchCmd = "%s -i %d".format(cmd, i)
      val sshCmd = s.hostName match {
        case "localhost" => launchCmd + " < /dev/null > /dev/null &"
        case _ => """ssh %s '$SHELL -l -c "%s < /dev/null > /dev/null &"'""".format(s.hostName, launchCmd)
      }
      info("launch command:%s", sshCmd)
      Shell.exec(sshCmd, applyQuotation = false)
    }

    // Launch SilkClients on each host


  }

  @command(description="Shut down silk cluster")
  def stop {
    // Find ZK servers
    val zkServers = defaultZKServers
    for((s, i) <- zkServers.zipWithIndex) {
      if(isAvailable(Seq(s))) {
        // shutdown the zookeeper server


      }
    }

    // Get Akka Actor Addresses of SilkClient

    // Send Termination signal


  }


  @command(description="start a zookeeper server")
  def zkStart(@option(prefix="-i", description="zkHost index to launch")
              id:Int=0,
              @argument zkHosts:Array[String]) {

    info("look up existing zookeeper server")

    // Parse zkHosts
    val server = zkHosts.map(ZkEnsembleHost(_)).toSeq

    val serverString = server.map(_.serverName).mkString(",")

    val isCluster = server.length > 1

    // Find ZooKeeperServer at localhost
    assert(id < zkHosts.length, "invalid zkhost id: %d".format(id))

    val zkHost : ZkEnsembleHost = server(id)
    if(!isAvailable(Seq(zkHost))) {
      try {
        info("Starting a new zookeeper server")

        val t = Executors.newFixedThreadPool(2)
        val config = ZooKeeper.buildQuorumConfig(id, server)
        val main = if(isCluster) new ZkQuorumPeer else new ZkStandalone
        t.submit(new Runnable() {
          def run {
            // start a new ZookeeperServer
            main.run(config) // This awaits the zookeeper termination
            info("shutdown the zookeeper server")
          }
        })

        t.submit(new Runnable() {
          def run {
            // start a client
            val client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", new ExponentialBackoffRetry(1000, 10))
            try {
              client.start

              val p = "/xerial/silk/zk/status"

              val path = new EnsurePath(p)
              path.ensure(client.getZookeeperClient)
              client.setData().forPath(p, "started".getBytes)
              while(true) {
                try {
                  val status = client.getData().watched().forPath(p)
                  val s = new String(status)
                  if(s == "terminate") {
                    main.shutdown
                    return
                  }
                }
                catch {
                  case e:Exception => error(e.getMessage)
                }
              }
            }
            finally {
              info("closing the client connection")
              client.close()
            }
          }
        })

        // await the termination
        t.shutdown()
        while(!t.awaitTermination(1, TimeUnit.SECONDS)) {}
        info("terminated")
      }
      catch {
        case e => warn("error occurred: %s", e.getMessage)
        e.printStackTrace
      }
    }
    else {
      info("ZooKeeper is already running at %s", zkHost)
    }
  }

  @command(description="terminate a zookeeper server")
  def zkStop(@option(prefix="-i", description="zkHost index to launch")
             id:Int=0,
             @argument zkHost:String="127.0.0.1") {

    val zkh = ZkEnsembleHost(zkHost)

    if(isAvailable(Seq(zkh))) {
      val client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", new ExponentialBackoffRetry(30, 10))
      try {
        client.start
        val path = new EnsurePath("/xerial/silk/zk/status")
        path.ensure(client.getZookeeperClient)
        info("write termination signal")
        client.setData().forPath("/xerial/silk/zk/status", "terminate".getBytes)
      }
      finally {
        client.close()
      }
    }

  }

}