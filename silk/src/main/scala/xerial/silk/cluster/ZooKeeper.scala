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
import org.apache.log4j.{PatternLayout, Appender, BasicConfigurator}
import xerial.lens.cui.{argument, option, command}
import xerial.core.util.{CommandLineTokenizer, Shell}
import com.google.common.io.Files
import java.util.concurrent.{TimeUnit, Executors, ExecutorService}
import com.netflix.curator.utils.EnsurePath
import com.netflix.curator.test.ByteCodeRewrite
import com.netflix.curator.framework.state.{ConnectionState, ConnectionStateListener}
import xerial.silk.util.Log4jUtil

/**
 * Interface to access ZooKeeper
 *
 * @author leo
 */
object ZooKeeper extends Logger {


  class ZkEnsembleHost(val hostName: String, val quorumPort: Int = config.zk.quorumPort, val leaderElectionPort: Int = config.zk.leaderElectionPort, val clientPort : Int = config.zk.clientPort) {
    override def toString = name
    def clientAddress = "%s:%s".format(hostName, clientPort)
    def serverString = "%s:%s".format(hostName, quorumPort)
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


  def readHostsFile(fileName:String) : Option[Seq[ZkEnsembleHost]] =
    readHostsFile(new File(fileName))

  def readHostsFile(file:File) : Option[Seq[ZkEnsembleHost]] = {
    if (!file.exists()) {
      warn("file %s not found", file)
      None
    }
    else {
      info("Reading %s", file)
      val r = for {
        (l, i) <- Source.fromFile(file).getLines().toSeq.zipWithIndex
        h <- l.trim match {
          case ZkEnsembleHost(z) => Some(z)
          case _ =>
            warn("invalid line (%d) in %s: %s", i + 1, file, l)
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
    val dataDir = new File(config.zk.dataDir, "server.%d".format(id))
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
    properties.setProperty("tickTime", config.zk.tickTime.toString)
    properties.setProperty("initLimit", config.zk.initLimit.toString)
    properties.setProperty("syncLimit", config.zk.syncLimit.toString)
    val dataDir = new File(config.zk.dataDir, "server.%d".format(id))
    if(!dataDir.exists)
      dataDir.mkdirs()
    properties.setProperty("dataDir", dataDir.getCanonicalPath)
    properties.setProperty("clientPort", config.zk.clientPort.toString)
    if (isCluster) {
      for ((h, hid) <- zkHosts.zipWithIndex) {
        properties.setProperty("server." + hid, "%s:%d:%d".format(h.hostName, h.quorumPort, h.leaderElectionPort))
      }
    }
    val peerConfig: QuorumPeerConfig = new QuorumPeerConfig
    peerConfig.parseProperties(properties)
    peerConfig
  }

  def isAvailable(server:ZkEnsembleHost) : Boolean = isAvailable(Seq(server))

  def isAvailable(servers:Seq[ZkEnsembleHost]) : Boolean = {
    val cs = servers.map(_.clientAddress).mkString(",")
    // Try to connect the ZooKeeper ensemble using a short delay
    info("Checking the availability of zookeeper servers")

    val available = Log4jUtil.withLogLevel(org.apache.log4j.Level.ERROR) {
      val client = new CuratorZookeeperClient(cs, 600, 150, null, new ExponentialBackoffRetry(10, 2))
      try
      {
        client.start
        client.blockUntilConnectedOrTimedOut()
      }
      catch {
        case e:Exception =>
          warn("no server is found: %s", e.getMessage)
          false
      }
      finally {
        client.close
      }
    }

    if(!available)
      info("No zookeeper server is found")
    else
      info("Found zookeeper server(s)")

    available
  }





  def defaultZKServers : Seq[ZkEnsembleHost] = {
    // read zkServer lists from $HOME/.clio/zkhosts file
    val ensembleServers : Seq[ZkEnsembleHost] = readHostsFile(ZK_HOSTS) getOrElse {
      info("pick up candidates of zookeeper servers from %s", SILK_HOSTS)
      val randomHosts = readHostsFile(SILK_HOSTS) filter { hosts => hosts.length >= 3} map { hosts =>
        hosts.take(3) // use first three hosts as zk servers
      }
      randomHosts.getOrElse {
        warn("Not enough servers found in %s file (required more than 3 servers). Use localhost as a single zookeeper master", SILK_HOSTS)
        Seq(new ZkEnsembleHost(localhost.address))
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

  /**
   * This code is a fix for MXBean unregister problem: https://github.com/Netflix/curator/issues/121
   */
  ByteCodeRewrite.apply()


  import ZooKeeper._


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


  @command(description="start a zookeeper server in the local machine")
  def zkStart(@option(prefix="-i", description="zkHost index to launch")
              id:Int=0,
              @argument(description = "list of the servers in your zookeeper ensemble")
              zkHosts:Array[String] = Array(localhost.address)) {

    // Parse zkHosts
    val server = zkHosts.map(ZkEnsembleHost(_)).toSeq

    val isCluster = server.length > 1
    // Find ZooKeeperServer at localhost
    assert(id < server.length, "invalid zkhost id: %d".format(id))

    val zkHost : ZkEnsembleHost = server(id)
    if(!isAvailable(zkHost)) {
      try {
        info("Starting a new zookeeper server")
        val t = Executors.newFixedThreadPool(2)
        val quorumConfig = ZooKeeper.buildQuorumConfig(id, server)
        val main = if(isCluster) new ZkQuorumPeer else new ZkStandalone
        t.submit(new Runnable() {
          def run {
            // start a new ZookeeperServer
            main.run(quorumConfig) // This awaits the zookeeper termination
            info("Shutdown the zookeeper server")
          }
        })

        t.submit(new Runnable() {
          def run {
            // start a client that listens status changes
            val client = CuratorFrameworkFactory.newClient(zkClientAddr, new ExponentialBackoffRetry(1000, 10))
            client.getConnectionStateListenable.addListener(simpleConnectionListener)
            try {
              client.start
              val p = config.zk.statusPath
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
              debug("closing the client connection")
              client.close()
            }
          }
        })

        // await the termination
        t.shutdown()
        while(!t.awaitTermination(1, TimeUnit.SECONDS)) {}
        info("Terminated")
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

  private val simpleConnectionListener = new ConnectionStateListener {
    def stateChanged(client: CuratorFramework, newState: ConnectionState) {
      debug("connection state changed: %s", newState.name)
    }
  }

  private val zkClientAddr = "%s:%s".format(localhost.address, config.zk.clientPort)

  @command(description="terminate a zookeeper server")
  def zkStop(@option(prefix="-i", description="zkHost index to launch")
             id:Int=0,
             @argument zkHost:String=localhost.address) {

    val zkh = ZkEnsembleHost(zkHost)
    if(isAvailable(zkh)) {
      val client = CuratorFrameworkFactory.newClient(zkClientAddr, new ExponentialBackoffRetry(30, 10))
      client.getConnectionStateListenable.addListener(simpleConnectionListener)
      try {
        client.start
        val path = config.zk.statusPath
        new EnsurePath(path).ensure(client.getZookeeperClient)
        info("write termination signal")
        client.setData().forPath(path, "terminate".getBytes)
      }
      finally {
        client.close()
      }
    }
  }

}