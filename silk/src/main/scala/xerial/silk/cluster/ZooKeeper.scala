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
import com.netflix.curator.{utils, CuratorZookeeperClient}
import com.netflix.curator.retry.ExponentialBackoffRetry
import io.Source
import xerial.silk.DefaultMessage
import org.apache.log4j.{PatternLayout, Appender, BasicConfigurator}
import xerial.lens.cui.{argument, option, command}
import xerial.core.util.{DataUnit, CommandLineTokenizer, Shell}
import com.google.common.io.Files
import java.util.concurrent.{TimeUnit, Executors, ExecutorService}
import com.netflix.curator.utils.EnsurePath
import com.netflix.curator.test.ByteCodeRewrite
import com.netflix.curator.framework.state.{ConnectionState, ConnectionStateListener}
import xerial.silk.util.Log4jUtil
import xerial.silk.core.SilkSerializer
import com.netflix.curator.framework.api.CuratorWatcher
import org.apache.zookeeper.{CreateMode, WatchedEvent}
import xerial.silk.cluster.SilkClient.Terminate

/**
 * Interface to access ZooKeeper
 *
 * @author leo
 */
object ZooKeeper extends Logger {

  /**
   * Zookeeper ensemble host
   * @param hostName
   * @param quorumPort
   * @param leaderElectionPort
   * @param clientPort
   */
  class ZkEnsembleHost(val hostName: String, val quorumPort: Int = config.zk.quorumPort, val leaderElectionPort: Int = config.zk.leaderElectionPort, val clientPort: Int = config.zk.clientPort) {
    override def toString = name
    def clientAddress = "%s:%s".format(hostName, clientPort)
    def name = "%s:%s:%s".format(hostName, quorumPort, leaderElectionPort)
  }

  object ZkEnsembleHost {
    def apply(s: String): ZkEnsembleHost = {
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


  /**
   * Read hosts file
   * @param fileName
   * @return
   */
  def readHostsFile(fileName: String): Option[Seq[ZkEnsembleHost]] =
    readHostsFile(new File(fileName))

  /**
   * Read hosts file
   * @param file
   * @return
   */
  def readHostsFile(file: File): Option[Seq[ZkEnsembleHost]] = {
    if (!file.exists()) {
      debug("file %s not found", file)
      None
    }
    else {
      info("Reading %s", file)
      val r = for {
        (l, i) <- Source.fromFile(file).getLines().toSeq.zipWithIndex
        h <- l.trim match {
          case z if z.startsWith("#") => None // comment line
          case ZkEnsembleHost(z) => Some(z)
          case _ =>
            warn("invalid line (%d) in %s: %s", i + 1, file, l)
            None
        }
      }
      yield h
      val hosts = r.toSeq
      if (hosts.length == 0)
        None
      else
        Some(hosts)
    }
  }

  /**
   * Write myid file necessary for launching zookeeper ensemble peer
   * @param id
   */
  private[cluster] def writeMyID(id: Int) {
    val dataDir = new File(config.zk.dataDir, "server.%d".format(id))
    if (!dataDir.exists)
      dataDir.mkdirs()

    val myIDFile = new File(dataDir, "myid")
    info("creating myid file at: %s", myIDFile)
    if (!myIDFile.exists()) {
      Files.write("%d".format(id).getBytes, myIDFile)
    }
  }


  /**
   * Build a zookeeper cluster configuration
   * @param id id in zkHosts
   * @param zkHosts zookeeper hosts
   * @return
   */
  private[cluster] def buildQuorumConfig(id: Int, zkHosts: Seq[ZkEnsembleHost]): QuorumPeerConfig = {

    val isCluster = zkHosts.length > 1

    if (isCluster) {
      info("write myid: %d", id)
      ZooKeeper.writeMyID(id)
    }

    val zkHost = zkHosts(id)
    val properties: Properties = new Properties
    properties.setProperty("tickTime", config.zk.tickTime.toString)
    properties.setProperty("initLimit", config.zk.initLimit.toString)
    properties.setProperty("syncLimit", config.zk.syncLimit.toString)
    val dataDir = new File(config.zk.dataDir, "server.%d".format(id))
    if (!dataDir.exists)
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

  /**
   * Check the availability of the zookeeper servers
   * @param server
   * @return
   */
  def isAvailable(server: ZkEnsembleHost): Boolean = isAvailable(Seq(server))

  /**
   * Check the availability of the zookeeper servers
   * @param servers
   * @return
   */
  def isAvailable(servers: Seq[ZkEnsembleHost]): Boolean = isAvailable(servers.map(_.clientAddress).mkString(","))

  /**
   * Check the availability of the zookeeper servers
   * @param serverString comma separated list of (addr):(zkClientPort)
   * @return
   */
  def isAvailable(serverString: String): Boolean =  {
    // Try to connect the ZooKeeper ensemble using a short delay
    info("Checking the availability of zookeeper: %s", serverString)
    val available = Log4jUtil.withLogLevel(org.apache.log4j.Level.ERROR) {
      val client = new CuratorZookeeperClient(serverString, 600, 150, null, new ExponentialBackoffRetry(10, 2))
      try {
        client.start
        client.blockUntilConnectedOrTimedOut()
      }
      catch {
        case e: Exception =>
          warn("No zookeeper is found at %s", e.getMessage)
          false
      }
      finally {
        client.close
      }
    }

    if (!available)
      info("Zookeeper is not running at %s", serverString)
    else
      info("Found zookeeper: %s", serverString)

    available
  }


  /**
   * Get the default zookeeper servers
   * @return
   */
  def defaultZKServers: Seq[ZkEnsembleHost] = {
    // read zkServer lists from $HOME/.silk/zkhosts file
    val ensembleServers: Seq[ZkEnsembleHost] = readHostsFile(ZK_HOSTS) getOrElse {
      info("Picking up candidates of zookeeper servers from %s", SILK_HOSTS)
      val randomHosts = readHostsFile(SILK_HOSTS) filter {
        hosts => hosts.length >= 3
      } map {
        hosts =>
          hosts.take(3) // use first three hosts as zk servers
      }
      randomHosts.getOrElse {
        warn("Not enough servers found in %s file (required more than 3 servers). Using localhost as a single zookeeper master", SILK_HOSTS)
        Seq(new ZkEnsembleHost(localhost.address))
      }
    }

    info("Selected zookeeper servers: %s", ensembleServers.mkString(", "))
    ensembleServers
  }

  def defaultZKServerAddr : String = defaultZKServers.map(_.clientAddress).mkString(",")


  /**
   * An interface for launching zookeeper
   */
  private[cluster] trait ZkServer {
    def run(config: QuorumPeerConfig): Unit
    def shutdown: Unit
  }

  /**
   * An instance of a clustered zookeeper
   */
  private[cluster] class ZkQuorumPeer extends QuorumPeerMain with ZkServer {
    def run(config: QuorumPeerConfig): Unit = {
      runFromConfig(config)
    }
    def shutdown {
      quorumPeer.shutdown
    }
  }

  /**
   * A standalone zookeeper server
   */
  private[cluster] class ZkStandalone extends ZooKeeperServerMain with ZkServer {
    def run(config: QuorumPeerConfig): Unit = {
      val sConfig = new ServerConfig
      sConfig.readFrom(config)
      runFromConfig(sConfig)
    }
    override def shutdown {
      super.shutdown
    }
  }


  private[cluster] val simpleConnectionListener = new ConnectionStateListener {
    def stateChanged(client: CuratorFramework, newState: ConnectionState) {
      debug("connection state changed: %s", newState.name)
    }
  }


  def withZkClient[U](zkServers: Seq[ZkEnsembleHost])(f: CuratorFramework => U): U =
    withZkClient(zkServers.map(_.clientAddress).mkString(","))(f)

  def withZkClient[U](zkServerAddr: String)(f: CuratorFramework => U): U = {
    val c = CuratorFrameworkFactory.newClient(zkServerAddr, new ExponentialBackoffRetry(3000, 10))
    c.start()
    c.getConnectionStateListenable.addListener(simpleConnectionListener)
    try {
      f(c)
    }
    finally {
      c.close();
    }
  }

}

/**
 * cluster management command sets
 * @author leo
 */
class ClusterCommand extends DefaultMessage with Logger {

  /**
   * This code is a fix for MXBean unregister problem: https://github.com/Netflix/curator/issues/121
   */
  ByteCodeRewrite.apply()

  xerial.silk.suppressLog4jwarning

  import ZooKeeper._


  @command(description = "Start up silk cluster")
  def start {

    // Find ZK servers
    val zkServers = defaultZKServers

    if (isAvailable(zkServers)) {
      info("Found zookeepers: %s", zkServers.mkString(","))
    }

    val zkHostsString = zkServers.map {
      _.name
    }.mkString(" ")
    val cmd = "silk cluster zkStart %s".format(zkHostsString)
    // login to each host, then launch zk
    info("Checking individual zookeepers")
    for ((s, i) <- zkServers.zipWithIndex.par) {
      if (!isAvailable(s)) {
        // login and launch the zookeeper server
        val launchCmd = "%s -i %d".format(cmd, i)
        val sshCmd = s.hostName match {
          case "localhost" => launchCmd + " < /dev/null > /dev/null &"
          case _ => """ssh %s '$SHELL -l -c "%s < /dev/null > /dev/null &"'""".format(s.hostName, launchCmd)
        }
        debug("Launch command:%s", sshCmd)
        Shell.exec(sshCmd, applyQuotation = false)
      }
    }

    // Prepare a new zookeeper connection

    info("Connecting to zookeeper: %s", zkServers)
    withZkClient(zkServers) {
      zkCli =>
        new EnsurePath(config.zk.clusterNodePath).ensure(zkCli.getZookeeperClient)
        // Launch SilkClients on each host
        for (host <- ClusterManager.defaultHosts().par) {
          info("Launch a SilkClient at %s", host)
          val zkServerAddr = zkServers.map(_.clientAddress).mkString(",")
          val launchCmd = "silk cluster startClient -n %s %s".format(host.name, zkServerAddr)
          val cmd = """ssh %s '$SHELL -l -c "%s < /dev/null > /dev/null &"'""".format(host.address, launchCmd)
          debug("Launch command:%s", cmd)
          Shell.exec(cmd, applyQuotation = false)
        }
    }

  }

  @command(description = "Shut down silk cluster")
  def stop {
    // Find ZK servers
    val zkServers = defaultZKServers
    if (isAvailable(zkServers)) {
      withZkClient(zkServers) {
        zkCli =>
        // Get Akka Actor Addresses of SilkClient
          val children = zkCli.getChildren.forPath(config.zk.clusterNodePath)
          import collection.JavaConversions._
          for (c <- children) {
            val data = zkCli.getData.forPath(config.zk.clusterNodePath + "/" + c)
            val m = SilkSerializer.deserializeAny(data).asInstanceOf[MachineResource]
            val sc = SilkClient.getClientAt(m.host.address)
            info("sending SilkClient termination signal to %s", m.host.address)
            sc ! Terminate
          }

          // Stop the zookeeper servers
          val path = config.zk.statusPath
          new EnsurePath(path).ensure(zkCli.getZookeeperClient)
          info("Send zookeeper termination signal")
          zkCli.setData().forPath(path, "terminate".getBytes)
      }
    }
  }

  @command(description = "list clients in cluster")
  def list {
    val zkServers = defaultZKServers
    if (isAvailable(zkServers)) {
      withZkClient(zkServers) {
        zkCli =>
          val children = zkCli.getChildren.forPath(config.zk.clusterNodePath)
          import collection.JavaConversions._
          for (c <- children) {
            val data = zkCli.getData.forPath(config.zk.clusterNodePath + "/" + c)
            val m = SilkSerializer.deserializeAny(data).asInstanceOf[MachineResource]
            println("%s\tCPU:%d\tmemory:%s".format(m.host.prefix, m.numCPUs, DataUnit.toHumanReadableFormat(m.memory)))
          }
      }
    }
    else {
      error("No zookeeper is running. Run 'silk cluster start' first.")
    }
  }


  @command(description = "start SilkClient")
  def startClient(@option(prefix = "-n", description = "hostname to use")
                  hostName: String = localhost.prefix,
                  @argument(description = "list of the servers in your zookeeper ensemble")
                  zkHosts: Option[String] = None) {

    val z = zkHosts getOrElse {
      error("No zkHosts is given. Use the default zookeeper addresses")
      defaultZKServerAddr
    }

    if(!isAvailable(z)) {
      error("No zookeeper is running. RUn 'silk cluster start' first.")
      return
    }

    withZkClient(z) {
      zkCli =>
        new EnsurePath(config.zk.clusterNodePath).ensure(zkCli.getZookeeperClient)
        val nodePath = config.zk.clusterNodePath + "/%s".format(hostName)
        val clusterEntry = zkCli.checkExists().forPath(nodePath)
        if (clusterEntry == null || SilkClient.getClientAt(localhost.address).isTerminated) {
          // launch
          val m = MachineResource.thisMachine
          val mSer = SilkSerializer.serialize(m)
          zkCli.create().withMode(CreateMode.EPHEMERAL).forPath(nodePath, mSer)
          info("start SilkClient on machine %s", m)
          SilkClient.startClient
        }
        else {
          info("SilkClient is already running")
        }
    }

  }


  @command(description = "start a zookeeper server in the local machine")
  def zkStart(@option(prefix = "-i", description = "zkHost index to launch")
              id: Int = 0,
              @argument(description = "list of the servers in your zookeeper ensemble")
              zkHosts: Array[String] = Array(localhost.address)) {

    // Parse zkHosts
    val server = zkHosts.map(ZkEnsembleHost(_)).toSeq

    val isCluster = server.length > 1
    // Find ZooKeeperServer at localhost
    assert(id < server.length, "invalid zkhost id: %d".format(id))

    val zkHost: ZkEnsembleHost = server(id)
    if (!isAvailable(zkHost)) {
      try {
        info("Starting a new zookeeper")
        val t = Executors.newFixedThreadPool(2)
        val quorumConfig = ZooKeeper.buildQuorumConfig(id, server)
        val main = if (isCluster) new ZkQuorumPeer else new ZkStandalone
        t.submit(new Runnable() {
          def run {
            // start a new ZookeeperServer
            main.run(quorumConfig) // This awaits the zookeeper termination
            info("Shutting down the zookeeper")
          }
        })

        t.submit(new Runnable() {
          def run {
            // start a client that listens status changes
            withZkClient(zkClientAddr) {
              client =>
                val p = config.zk.statusPath
                val path = new EnsurePath(p)
                path.ensure(client.getZookeeperClient)
                client.setData().forPath(p, "started".getBytes)
                while (true) {
                  try {
                    val status = client.getData().watched().forPath(p)
                    val s = new String(status)
                    if (s == "terminate") {
                      main.shutdown
                      return
                    }
                  }
                  catch {
                    case e: Exception => error(e.getMessage)
                  }
                }
            }
          }
        })

        // await the termination
        t.shutdown()
        while (!t.awaitTermination(1, TimeUnit.SECONDS)) {}
        info("Terminated")
      }
      catch {
        case e => warn("error occurred: %s", e.getMessage)
        e.printStackTrace
      }
    }
    else {
      info("zookeeper is already running at %s", zkHost)
    }
  }


  private val zkClientAddr = "%s:%s".format(localhost.address, config.zk.clientPort)

  @command(description = "terminate a zookeeper server")
  def zkStop(@option(prefix = "-i", description = "zkHost index to launch")
             id: Int = 0,
             @argument zkHost: String = localhost.address) {

    val zkh = ZkEnsembleHost(zkHost)
    if (isAvailable(zkh)) {
      withZkClient(zkClientAddr) {
        client =>
          val path = config.zk.statusPath
          new EnsurePath(path).ensure(client.getZookeeperClient)
          info("Write termination signal")
          client.setData().forPath(path, "terminate".getBytes)
      }
    }
  }

}