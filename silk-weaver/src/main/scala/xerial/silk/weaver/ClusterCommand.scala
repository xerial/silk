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
// ClusterCommand.scala
// Since: 2012/12/19 11:54 AM
//
//--------------------------------------

package xerial.silk.weaver

import xerial.core.log.{LoggerFactory, LogLevel, Logger}
import xerial.lens.cui.{argument, option, command}
import xerial.core.util.{DataUnit, Shell}
import java.util.concurrent.{TimeoutException, TimeUnit, Executors}
import java.io.File
import xerial.silk.framework._
import xerial.silk.cluster._
import SilkClient.{Terminate}
import xerial.silk.util.ThreadUtil.ThreadManager
import java.util.concurrent.atomic.AtomicInteger
import xerial.silk.util.Log4jUtil
import xerial.silk.framework.Node
import xerial.silk.cluster.SilkClient.SilkClientRef
import xerial.silk.cluster.ZkConfig
import xerial.silk.cluster.rm.ClusterNodeManager


/**
 * Cluster management commands
 * @author Taro L. Saito
 */
class ClusterCommand extends DefaultMessage with Logger {

  Log4jUtil.suppressLog4jwarning


  import ZooKeeper._
  import SilkCluster._


  override def default = {
    info("Checking the status of silk cluster")
    list()
  }


  private def toUnixPath(f: File) = {
    val p = f.getCanonicalPath
    p.replaceAllLiterally(File.separator, "/")
  }

  @command(description = "Start up silk cluster")
  def start {

    val f = SilkClusterFramework.default

    // login to each host, then launch zk
    info("Checking individual ZooKeepers")
    val cmd = "silk cluster zkStart %s".format(f.zkServerString)
    f.zkServers.zipWithIndex.par.foreach {
      case (s, i) =>
        if (!isAvailable(s)) {
          // login and launch the zookeeper server
          val launchCmd = "%s -i %d".format(cmd, i)
          val log = f.logFile(s.host.prefix)
          val sshCmd = """ssh %s '$SHELL -l -c "mkdir -p %s; %s < /dev/null >> %s 2>&1 &"'""".format(s.host.address, toUnixPath(log.getParentFile), launchCmd, toUnixPath(log))
          debug(s"Launch command:$sshCmd")
          info(s"Start ZooKeeper at ${s.host}")
          Shell.exec(sshCmd)
        }
    }

    val zkConnectString = f.zkConnectString
    info(s"Connecting to ZooKeeper: $zkConnectString")
    for (zk <- f.defaultZkClient whenMissing {
      warn(s"Failed to connect ZooKeeper at $zkConnectString")
    }) {
      // Launch SilkClients on each host
      for (host <- SilkCluster.defaultHosts(f.config.home.silkHosts).par) {
        info(s"Launch a SilkClient at ${host.prefix}")
        val launchCmd = s"silk cluster startClient -l ${LoggerFactory.getDefaultLogLevel} --name ${host.name} --address ${host.address} --zk ${f.zkConnectString}"
        val log = f.logFile(host.prefix)
        val cmd = """ssh %s '$SHELL -l -c "mkdir -p %s; %s < /dev/null >> %s 2>&1 &"'""".format(host.address, toUnixPath(log.getParentFile), launchCmd, toUnixPath(log))
        debug(s"Launch command:$cmd")
        Shell.exec(cmd)
      }
    }
  }


  @command(description = "Shut down silk cluster")
  def stop {
    val f = SilkClusterFramework.default

    if(!ZooKeeper.isAvailable(f.zkConnectString)) {
      warn("No zookeeper is found")
      return
    }
    val config = f.config
    // Find ZK servers
    for (zk <- f.defaultZkClient whenMissing { warn("No Zookeeper server is found") }) {

      // Set a shutdown flag to clusterStatePath to disable new master election
      zk.set(config.zk.clusterStatePath, "shutdown".getBytes)
      // Stop SilkClients
      stopClients(zk)
      // Deletes the master record so that SilkClient can await the start up of a new SilkMaster at the next start time
      zk.remove(config.zk.masterInfoPath)

      // Stop the zookeeper servers by recording a termination signal to ZooKeeper
      info("Sending zookeeper termination signal")
      zk.set(config.zk.statusPath, "terminate".getBytes)
    }

  }

  import ClusterCommand._

  private def stopClients(zk:ZooKeeperClient) {

    def kill(c:Node) {
      // TODO kill the client process directory
      val cmd = s"""ssh ${c.host.name} "kill ${c.pid} > /dev/null 2>&1" """
      debug(s"Send a kill command:${cmd}")
      Shell.exec(cmd)
    }

    for {ci <- collectClientInfo(zk)
         actorSystem <- ActorService(SilkCluster.localhost)
         sc <- SilkClient.remoteClient(actorSystem, ci.host, ci.clientPort)} {
      debug(s"Sending SilkClient termination signal to ${ci.host.prefix}")
      try {
        sc ? Terminate
        kill(ci)
        info(s"Terminated SilkClient at ${ci.host.prefix}")
      }
      catch {
        case e: TimeoutException => {
          warn(e)
          kill(ci)
        }
      }
    }
  }

  @command(description = "Stop all SilkClients")
  def stopClients {
    val f = SilkClusterFramework.default

    for(zk <- f.defaultZkClient)
      stopClients(zk)
  }


  @command(description = "list clients in cluster")
  def list(@option(prefix="--status", description="check status")
           checkStatus:Boolean = false)  {

    val f = SilkClusterFramework.default

    if (!ZooKeeper.isAvailable(f.zkConnectString)) {
      warn("No zookeeper is found. Run `silk cluster start` first.")
      return
    }

    for(zk <- f.defaultZkClient; actorSystem <- ActorService(localhost)){
      val  nodes = ClusterCommand.collectClientInfo(zk)
      val master = MasterRecord.getMaster(zk)

      for(ci <- nodes.sortBy(_.name)) {
        val m = ci.resource
        val isMaster = master.exists(_.name == ci.name)

        for(rc <- SilkClient.remoteClient(actorSystem, ci.host, ci.clientPort)) {
          val status = try {
            import scala.concurrent.duration._
            rc.?(SilkClient.ReportStatus, 500.milliseconds)
            "OK"
          }
          catch {
            case e: TimeoutException => "timeout"
          }
          println(f"${ci.host.prefix}\tCPU:${m.numCPUs}\tmemory:${DataUnit.toHumanReadableFormat(m.memorySize)}\tpid:${ci.pid}%-5d\t${status}${if(isMaster) " (master)" else ""}")
        }
      }
    }
  }

  @command(description = "start SilkClient")
  def startClient(@option(prefix = "--name", description = "hostname to use")
                  hostName: String = localhost.prefix,
                  @option(prefix = "--address", description = "client address accessible from the other hosts")
                  address: String = localhost.address,
                  @option(prefix = "--zk", description = "list of the servers in your zookeeper ensemble")
                  zkHosts: Option[String] = None) {

    val f = SilkClusterFramework.default

    val z = zkHosts getOrElse {
      error("No zkHosts is given. Use the default zookeeper addresses")
      f.zkConnectString
    }

    ClusterSetup.startClient(f.config, Host(hostName, address), z) {
      env =>
        env.awaitTermination
    }
  }

  @command(description = "start SilkClient")
  def stopClient(@option(prefix = "-n", description = "hostname to use")
                 hostName: String = localhost.prefix) {

    val f = SilkClusterFramework.default

    if (!ZooKeeper.isAvailable(f.zkConnectString)) {
      error("No zookeeper is running. Run 'silk cluster start' first.")
      return
    }

    for {
      actorSystem <- ActorService(localhost)
      sc <- SilkClient.remoteClient(actorSystem, Host(hostName), f.config.cluster.silkClientPort)
    }
      sc ! Terminate

  }

  @command(description = "start a zookeeper server in the local machine")
  def zkStart(@option(prefix = "-i", description = "zkHost index to launch")
              id: Int = 0,
              @argument(description = "list of the servers in your zookeeper ensemble")
              zkHosts: Array[String] = Array(localhost.address),
              @option(prefix = "-p", description = "client port")
              zkClientPort: Int = ZkConfig.defaultZkClientPort,
              @option(prefix = "--home", description = "silk home directory")
              silkHome: File = HomeConfig.defaultSilkHome
               ) {


    val f = new SilkClusterFramework {
      override val config = new SilkClusterFramework.ConfigBase {
        override val home = HomeConfig(
          silkHome = silkHome
        )
        override val zk = ZkConfig(
          clientPort = zkClientPort
        )
      }
    }

    val server = f.zkServers
    val isCluster = server.length > 1
    // Find ZooKeeperServer at localhost
    assert(id < server.length, s"invalid zkhost id:${id}, servers:${server.mkString(", ")}")

    val zkHost: ZkEnsembleHost = server(id)
    if (!isAvailable(s"${zkHost.host.address}:${zkClientPort}")) {
      try {
        val t = new ThreadManager(2)
        val quorumConfig = ZooKeeper.buildQuorumConfig(f.config.zk, id, server)
        val zkMain = if (isCluster) new ZkQuorumPeer else new ZkStandalone
        val counter = new AtomicInteger(0)
        t.submit{
          // start a new ZookeeperServer
          info("Starting a new zookeeper")
          counter.incrementAndGet()
          zkMain.run(quorumConfig) // This awaits the zookeeper termination
          info("Shutting down the ZooKeeper")
        }

        t.submit{
          try {
            while(counter.compareAndSet(1, 2)) { Thread.sleep(1000) }
            // start a client that listens status changes
            for (zk <- f.defaultZkClient) {
              info("Zookeeper is started")
              zk.set(f.config.zk.statusPath, "started".getBytes)
              zk.set(f.config.zk.clusterStatePath, "started".getBytes)

              var toContinue = true
              while(toContinue) {
                val status = zk.watchUpdate(f.config.zk.statusPath).map(new String(_))
                if (status.get == "terminate") {
                  toContinue = false
                }
              }
            }
            // Shutdown the zookeeper after closing the zookeeper connection
            zkMain.shutdown
          }
          catch {
            case e: Exception => error(e.getMessage)
          }
        }

        // await the termination
        t.join
        info("Terminated the ZooKeeper")
      }
      catch {
        case e: Exception => warn(e)
      }
    }
    else {
      info(s"Zookeeper is already running at $zkHost")
    }
  }

  //private val zkClientAddr = "%s:%s".format(localhost.address, config.zk.clientPort)

  @command(description = "terminate a zookeeper server")
  def zkStop(@argument zkHost: String = localhost.address,
             @option(prefix = "-p", description = "client port")
             zkClientPort: Int = ZkConfig.defaultZkClientPort) {

    val f = SilkClusterFramework.default

    for (zk <- zkClient(f.config.zk, s"$zkHost:$zkClientPort")) {
      info("Write termination signal")
      zk.set(f.config.zk.statusPath, "terminate".getBytes)
    }
  }

  @command(description = "list zookeeper entries")
  def zkls(@argument path: String = "/") {
    val f = SilkClusterFramework.default

    for (zk <- f.defaultZkClient; c <- zk.ls(path)) {
      println(c)
    }
  }

  @command(description = "list zookeeper entries")
  def zkget(@argument path: String = "/") {
    val f = SilkClusterFramework.default
    for (zk <- f.defaultZkClient; b <- zk.get(path)) {
      val obj = SilkSerializer.deserializeObj[AnyRef](b)
      println(obj.toString)
    }
  }

//  @command(description = "Set loglevel of silk clients")
//  def setLogLevel(@argument logLevel: LogLevel) {
//    val f = SilkClusterFramework.default
//    for (h <- f.hosts)
//      at(h) {
//        LoggerFactory.setDefaultLogLevel(logLevel)
//      }
//  }
//
//  @command(description = "monitor the logs of cluster nodes")
//  def log {
//    implicit val silk = Silk.testInit // TODO fixme
//    try {
//      for (h <- hosts)
//        at(h) {
//          // Insert a network logger
//          info("Insert a network logger")
//
//        }
//      // Await the keyboard input
//      Console.in.read()
//    }
//    finally {
//      for (h <- hosts) {
//        at(h) {
//          // Remove the logger
//        }
//      }
//    }
//  }

  @command(description = "Force killing the cluster instance")
  def kill {

    val f = SilkClusterFramework.default

    for (h : Host <- Host.readHostsFile(f.config.home.silkHosts)) {
      val cmd = """jps -m | grep SilkMain | grep -v kill | cut -f 1 -d " " | xargs -r kill"""
      info(s"Killing SilkMain processes in ${h}")
      Shell.execRemote(h.name, cmd)
    }
  }

  @command(description = "Exec a command on all hosts")
  def exec(@argument cmd: Array[String] = Array.empty) {

    val f = SilkClusterFramework.default

    val cmdLine = cmd.mkString(" ")
    for (h <- Host.readHostsFile(f.config.home.silkHosts)) {
      Shell.execRemote(h.name, cmdLine)
    }
  }


  def listServerStatus: Seq[(Node, String)] = {
    def getStatus(sc: SilkClientRef): String = {
      val s = try
        sc ? SilkClient.ReportStatus
      catch {
        case e: TimeoutException =>
          warn(s"request for ${sc.addr} is timed out")
          "No response"
      }
      s.toString
    }

    val f = SilkClusterFramework.default

    if (!ZooKeeper.isAvailable(f.zkConnectString)) {
      warn("No zookeeper is found")
      return Seq.empty
    }

    val s = for {
      zk <- f.defaultZkClient
      as <- ActorService(localhost)
      ci <- ClusterCommand.collectClientInfo(zk).par
      sc <- SilkClient.remoteClient(as, ci.host, ci.clientPort)
    } yield
      (ci, getStatus(sc))

    s.toSeq
  }



}


object ClusterCommand {

  def collectClientInfo(zkc: ZooKeeperClient): Seq[Node] = {
    val cm = new ClusterNodeManager with ZooKeeperService with SilkClusterFramework {
      val zk = zkc
    }
    cm.nodeManager.nodes
  }

}