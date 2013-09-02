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
import xerial.silk._
import xerial.silk.cluster._
import SilkClient.{Terminate, SilkClientRef}
import xerial.silk.framework.{Host, Node}
import xerial.silk.cluster.framework.{MasterRecord, ActorService, ZooKeeperService, ClusterNodeManager}
import xerial.silk.cluster._
import xerial.silk.framework.Node
import SilkClient.SilkClientRef
import xerial.silk.core.SilkSerializer
import xerial.silk.util.ThreadUtil.ThreadManager
import java.util.concurrent.atomic.AtomicInteger

/**
 * Cluster management commands
 * @author Taro L. Saito
 */
class ClusterCommand extends DefaultMessage with Logger {

  cluster.suppressLog4jwarning

  import ZooKeeper._

  private def logFile(hostName: String): File = new File(config.silkLogDir, "%s.log".format(hostName))

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

    // Find ZK servers
    val zkServers = config.zk.getZkServers
    val zkHostsString = zkServers.map(_.serverAddress).mkString(" ")

//    if (isAvailable(zkServers)) {
//      info(s"Found zookeepers: ${zkServers.mkString(",")}")
//    }

    val cmd = "silk cluster zkStart %s".format(zkHostsString)
    // login to each host, then launch zk
    info("Checking individual ZooKeepers")
    zkServers.zipWithIndex.par.foreach {
      case (s, i) =>
        if (!isAvailable(s)) {
          // login and launch the zookeeper server
          val launchCmd = "%s -i %d".format(cmd, i)
          val log = logFile(s.host.prefix)
          val sshCmd = """ssh %s '$SHELL -l -c "mkdir -p %s; %s < /dev/null >> %s 2>&1 &"'""".format(s.host.address, toUnixPath(log.getParentFile), launchCmd, toUnixPath(log))
          debug(s"Launch command:$sshCmd")
          info(s"Start ZooKeeper at ${s.host}")
          Shell.exec(sshCmd)
        }
    }

    val zkConnectString = config.zk.zkServersConnectString
    info(s"Connecting to ZooKeeper: $zkConnectString")
    for (zk <- defaultZkClient whenMissing {
      warn(s"Failed to connect ZooKeeper at $zkConnectString")
    }) {
      // Launch SilkClients on each host
      for (host <- cluster.defaultHosts().par) {
        info(s"Launch a SilkClient at ${host.prefix}")
        val zkServerAddr = zkServers.map(_.connectAddress).mkString(",")
        val launchCmd = s"silk cluster startClient -l ${LoggerFactory.getDefaultLogLevel} --name ${host.name} --address ${host.address} --zk $zkServerAddr"
        val log = logFile(host.prefix)
        val cmd = """ssh %s '$SHELL -l -c "mkdir -p %s; %s < /dev/null >> %s 2>&1 &"'""".format(host.address, toUnixPath(log.getParentFile), launchCmd, toUnixPath(log))
        debug(s"Launch command:$cmd")
        Shell.exec(cmd)
      }
    }
  }


  @command(description = "Shut down silk cluster")
  def stop {

    if (!isAvailable) {
      warn("No zookeeper is found")
      return
    }

    // Find ZK servers
    for (zk <- defaultZkClient whenMissing { warn("No Zookeeper server is found") }) {

      // Setting clusterSatePath to shutdown disables new master election
      zk.set(config.zk.clusterStatePath, "shutdown".getBytes)
      // Stop Clients
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
         actorSystem <- ActorService(localhost)
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
    for(zk <- defaultZkClient)
      stopClients(zk)
  }


  @command(description = "list clients in cluster")
  def list(@option(prefix="--status", description="check status")
           checkStatus:Boolean = false)  {

    if (!isAvailable) {
      warn("No zookeeper is found")
      return
    }

    for(zk <- defaultZkClient; actorSystem <- ActorService(localhost)){
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

    val z = zkHosts getOrElse {
      error("No zkHosts is given. Use the default zookeeper addresses")
      config.zk.zkServersConnectString
    }

    ClusterSetup.startClient(Host(hostName, address), z) {
      env =>
        env.clientRef.system.awaitTermination()
    }
  }

  @command(description = "start SilkClient")
  def stopClient(@option(prefix = "-n", description = "hostname to use")
                 hostName: String = localhost.prefix) {

    if (!isAvailable) {
      error("No zookeeper is running. Run 'silk cluster start' first.")
      return
    }

    for {
      actorSystem <- ActorService(localhost)
      sc <- SilkClient.remoteClient(actorSystem, Host(hostName))
    }
      sc ! Terminate

  }

  @command(description = "start a zookeeper server in the local machine")
  def zkStart(@option(prefix = "-i", description = "zkHost index to launch")
              id: Int = 0,
              @argument(description = "list of the servers in your zookeeper ensemble")
              zkHosts: Array[String] = Array(localhost.address),
              @option(prefix = "-p", description = "client port")
              zkClientPort: Int = config.zk.clientPort,
              @option(prefix = "--home", description = "silk home directory")
              silkHome: File = Config.defaultSilkHome
               ) {

    val server: Seq[ZkEnsembleHost] = {
      withConfig(Config(silkHome = silkHome, zk = ZkConfig(clientPort = zkClientPort))) {
        // Parse zkHosts
        zkHosts.map(ZkEnsembleHost(_)).toSeq
      }
    }

    val newConfig = Config(silkHome = silkHome, zk = ZkConfig(clientPort = zkClientPort, zkServers = Some(server)))

    withConfig(newConfig) {
      val isCluster = server.length > 1
      // Find ZooKeeperServer at localhost
      assert(id < server.length, "invalid zkhost id:%d, servers:%s".format(id, server.mkString(", ")))

      val zkHost: ZkEnsembleHost = server(id)
      if (!isAvailable(s"${zkHost.host.address}:${zkClientPort}")) {
        try {

          val t = new ThreadManager(2)
          val quorumConfig = ZooKeeper.buildQuorumConfig(id, server)
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
              for (zk <- defaultZkClient) {
                info("Zookeeper is started")
                zk.set(config.zk.statusPath, "started".getBytes)
                zk.set(config.zk.clusterStatePath, "started".getBytes)

                var toContinue = true
                while(toContinue) {
                  val status = zk.watchUpdate(config.zk.statusPath).map(new String(_))
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
  }


  private val zkClientAddr = "%s:%s".format(localhost.address, config.zk.clientPort)

  @command(description = "terminate a zookeeper server")
  def zkStop(@argument zkHost: String = localhost.address,
             @option(prefix = "-p", description = "client port")
             zkClientPort: Int = config.zk.clientPort) {

    for (zk <- zkClient(s"$zkHost:$zkClientPort")) {
      info("Write termination signal")
      zk.set(config.zk.statusPath, "terminate".getBytes)
    }
  }

  @command(description = "list zookeeper entries")
  def zkls(@argument path: String = "/") {
    for (zk <- defaultZkClient; c <- zk.ls(path)) {
      println(c)
    }
  }

  @command(description = "list zookeeper entries")
  def zkget(@argument path: String = "/") {
    for (zk <- defaultZkClient; b <- zk.get(path)) {
      val obj = SilkSerializer.deserializeObj[AnyRef](b)
      println(obj.toString)
    }
  }

  @command(description = "Set loglevel of silk clients")
  def setLogLevel(@argument logLevel: LogLevel) {
    for (h <- hosts)
      at(h) {
        LoggerFactory.setDefaultLogLevel(logLevel)
      }
  }

  @command(description = "monitor the logs of cluster nodes")
  def log {
    try {
      for (h <- hosts)
        at(h) {
          // Insert a network logger
          info("Insert a network logger")

        }
      // Await the keyboard input
      Console.in.read()
    }
    finally {
      for (h <- hosts) {
        at(h) {
          // Remove the logger
        }
      }
    }
  }

  @command(description = "Force killing the cluster instance")
  def kill {
    for (hosts <- readHostsFile(config.silkHosts); h <- hosts) {
      val cmd = """jps -m | grep SilkMain | grep -v kill | cut -f 1 -d " " | xargs -r kill"""
      info(s"Killing SilkMain processes in ${h.host}")
      Shell.execRemote(h.host.name, cmd)
    }
  }

  @command(description = "Exec a command on all hosts")
  def exec(@argument cmd: Array[String] = Array.empty) {
    val cmdLine = cmd.mkString(" ")
    for (hosts <- readHostsFile(config.silkHosts); h <- hosts) {
      Shell.execRemote(h.host.name, cmdLine)
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

    if (!isAvailable) {
      warn("No zookeeper is found")
      return Seq.empty
    }

    val s = for {
      zk <- defaultZkClient
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
    val cm = new ClusterNodeManager with ZooKeeperService {
      val zk = zkc
    }
    cm.nodeManager.nodes
  }

}