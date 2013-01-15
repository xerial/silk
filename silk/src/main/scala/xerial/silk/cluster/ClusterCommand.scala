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

package xerial.silk.cluster

import xerial.core.log.{LoggerFactory, LogLevel, Logger}
import xerial.lens.cui.{argument, option, command}
import xerial.core.util.{DataUnit, Shell}
import xerial.silk.core.Silk
import java.util.concurrent.{TimeoutException, TimeUnit, Executors}
import java.io.File
import xerial.silk._
import cluster.SilkClient.{Terminate, ClientInfo, SilkClientRef}

/**
 * Cluster management commands
 * @author Taro L. Saito
 */
class ClusterCommand extends DefaultMessage with Logger {

  xerial.silk.suppressLog4jwarning

  import ZooKeeper._

  private def logFile(hostName: String): File = new File(config.silkLogDir, "%s.log".format(hostName))

  override def default = {
    info("Checking the status of silk cluster")
    list
  }


  private def toUnixPath(f: File) = {
    val p = f.getCanonicalPath
    p.replaceAllLiterally(File.separator, "/")
  }

  @command(description = "Start up silk cluster")
  def start {

    // Find ZK servers
    val zkServers = config.zk.getZkServers

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
        val log = logFile(s.hostName)
        val sshCmd = """ssh %s '$SHELL -l -c "mkdir -p %s; %s < /dev/null >> %s 2>&1 &"'""".format(s.hostName, toUnixPath(log.getParentFile), launchCmd, toUnixPath(log))
        debug("Launch command:%s", sshCmd)
        info("Start zookeeper at %s", s.hostName)
        Shell.exec(sshCmd)
      }
    }

    info("Connecting to zookeeper: %s", zkServers)
    for(zk <- defaultZkClient whenMissing { warn("Failed to launch zookeeper at %s", zkHostsString)}) {
      // Launch SilkClients on each host
      for (host <- ClusterManager.defaultHosts().par) {
        info("Launch a SilkClient at %s", host.prefix)
        val zkServerAddr = zkServers.map(_.clientAddress).mkString(",")
        val launchCmd = "silk cluster startClient -n %s %s".format(host.name, zkServerAddr)
        val log = logFile(host.prefix)
        val cmd = """ssh %s '$SHELL -l -c "mkdir -p %s; %s < /dev/null >> %s 2>&1 &"'""".format(host.address, toUnixPath(log.getParentFile), launchCmd, toUnixPath(log))
        debug("Launch command:%s", cmd)
        Shell.exec(cmd)
      }
    }
  }


  @command(description = "Shut down silk cluster")
  def stop {
    // Find ZK servers

    for (zk <- defaultZkClient whenMissing { warn("No Zookeeper server is found") } ) {
      stopClients()
      // Stop the zookeeper servers
      info("Sending zookeeper termination signal")
      zk.set(config.zk.statusPath, "terminate".getBytes)
    }

  }

  @command(description = "Stop all SilkClients")
  def stopClients(@option(prefix = "-f") force: Boolean = false) {
    for (zk <- defaultZkClient; ci <- collectClientInfo(zk)) {
      // Get Akka Actor Addresses of SilkClient
      for (sc <- SilkClient.remoteClient(ci.host, ci.port)) {
        debug("Sending SilkClient termination signal to %s", ci.host.prefix)
        try {
          sc ? Terminate
          info("Terminated SilkClient at %s", ci.host.prefix)
        }
        catch {
          case e: TimeoutException => {
            warn(e)
            // TODO kill the client process directory
            val cmd = "ssh %s kill %d".format(ci.host.name, ci.pid)
            debug("Launch a kill command:%s", cmd)
            Shell.exec(cmd)
          }
        }
      }
    }
  }


  @command(description = "list clients in cluster")
  def list {
    for ((ci, status) <- listServerStatus) {
      val m = ci.m
      println("%s\tCPU:%d\tmemory:%s, pid:%d, status:%s".format(ci.host.prefix, m.numCPUs, DataUnit.toHumanReadableFormat(m.memory), ci.pid, status))
    }
  }

  @command(description = "start SilkClient")
  def startClient(@option(prefix = "-n", description = "hostname to use")
                  hostName: String = localhost.prefix,
                  @argument(description = "list of the servers in your zookeeper ensemble")
                  zkHosts: Option[String] = None) {

    val z = zkHosts getOrElse {
      error("No zkHosts is given. Use the default zookeeper addresses")
      config.zk.zkServersConnectString
    }

    SilkClient.startClient(Host(hostName, localhost.address))
  }

  @command(description = "start SilkClient")
  def stopClient(@option(prefix = "-n", description = "hostname to use")
                 hostName: String = localhost.prefix) {

    if (!isAvailable) {
      error("No zookeeper is running. Run 'silk cluster start' first.")
      return
    }

    for (sc <- SilkClient.remoteClient(Host(hostName)))
      sc ! Terminate

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
    assert(id < server.length, "invalid zkhost id:%d, servers:%s".format(id, server.mkString(", ")))

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
            for (zk <- defaultZkClient) {
              zk.set(config.zk.statusPath, "started".getBytes)
              while (true) {
                try {
                  val s = zk.watch(config.zk.statusPath)
                  val status = new String(s)
                  if (status == "terminate") {
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
        while (!t.awaitTermination(1, TimeUnit.SECONDS)) {
        }
        info("Terminated")
      }
      catch {
        case e => warn("error occurred: %s", e.getMessage)
        e.printStackTrace
      }
    }
    else {
      info("Zookeeper is already running at %s", zkHost)
    }
  }


  private val zkClientAddr = "%s:%s".format(localhost.address, config.zk.clientPort)

  @command(description = "terminate a zookeeper server")
  def zkStop(@argument zkHost: String = localhost.address) {

    val zkh = ZkEnsembleHost(zkHost)
    for (zk <- zkClient(zkh.clientAddress)) {
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
      println(new String(b))
    }
  }

  @command(description = "Set loglevel of silk clients")
  def setLogLevel(@argument logLevel: LogLevel) {
    for (h <- Silk.hosts)
      at(h) {
        LoggerFactory.setDefaultLogLevel(logLevel)
      }
  }

  @command(description = "monitor the logs of cluster nodes")
  def monitorLog {
    try {
      for (h <- Silk.hosts)
        at(h) {
          // Insert a network logger
          info("Insert a network logger")

        }
      // Await the keyboard input
      Console.in.read()
    }
    finally {
      for (h <- Silk.hosts) {
        at(h) {
          // Remove the logger
        }
      }
    }
  }


  def listServerStatus: Seq[(ClientInfo, String)] = {
    def getStatus(sc: SilkClientRef): String = {
      val s = try
        sc ? SilkClient.Status
      catch {
        case e: TimeoutException =>
          warn("request for %s is timed out", sc.addr)
          "No response"
      }
      s.toString
    }

    val s = for {
      zk <- defaultZkClient
      ci <- collectClientInfo(zk)
      sc <- SilkClient.remoteClient(ci.host, ci.port)
    } yield (ci, getStatus(sc))
    s getOrElse Seq.empty
  }


  private def collectClientInfo(zk: ZooKeeperClient): Seq[ClientInfo] = {
    zk.ls(config.zk.clusterNodePath).map {
      c => SilkClient.getClientInfo(zk, Host(c))
    }.collect {
      case Some(x) => x
    }
  }

}
