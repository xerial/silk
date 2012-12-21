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

import xerial.silk.DefaultMessage
import xerial.core.log.{LoggerFactory, LogLevel, Logger}
import com.netflix.curator.test.ByteCodeRewrite
import xerial.lens.cui.{argument, option, command}
import xerial.core.util.{DataUnit, Shell}
import com.netflix.curator.utils.EnsurePath
import xerial.silk.core.{Silk, SilkSerializer}
import org.apache.zookeeper.CreateMode
import java.util.concurrent.{TimeoutException, TimeUnit, Executors}
import xerial.silk.cluster.SilkClient.{ClientInfo, Terminate, Status}
import java.io.File
import com.netflix.curator.framework.CuratorFramework




/**
 * Cluster management commands
 * @author Taro L. Saito
 */
class ClusterCommand extends DefaultMessage with Logger {

  /**
   * This code is a fix for MXBean unregister problem: https://github.com/Netflix/curator/issues/121
   */
  ByteCodeRewrite.apply()

  xerial.silk.suppressLog4jwarning

  import ZooKeeper._
  import ClusterManager._

  private def logFile(hostName:String) : File = new File(SILK_LOGDIR, "%s.log".format(hostName))

  override def default = {
    info("Checking the status of silk cluster")
    list
  }


  private def toUnixPath(f:File) = {
    val p = f.getCanonicalPath
    p.replaceAllLiterally(File.separator, "/")
  }

  @command(description = "Start up silk cluster")
  def start {

    // Find ZK servers
    val zkServers = defaultZKServers

    if (isAvailable(zkServers)) {
      info("Found zookeepers: %s", zkServers.mkString(","))
    }


    val zkHostsString = zkServers.map { _.name }.mkString(" ")
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

    // Prepare a new zookeeper connection

    info("Connecting to zookeeper: %s", zkServers)
    withZkClient(zkServers) {
      zkCli =>
        new EnsurePath(config.zk.clusterNodePath).ensure(zkCli.getZookeeperClient)
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

        // TODO timing
        //listServerStatus(zkCli)
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
            getClientInfo(zkCli, c) map { ci =>
              val sc = SilkClient.getClientAt(ci.m.host.address)
              debug("Sending SilkClient termination signal to %s",ci.m.hostname)

              import akka.pattern.ask
              import akka.dispatch.Await
              import akka.util.Timeout
              import akka.util.duration._
              try {
                implicit val timeout = Timeout(3 seconds)
                val reply = (sc ? Terminate).mapTo[String]
                Await.result(reply, timeout.duration)
                info("Terminated SilkClient at %s", ci.m.hostname)
              }
              catch {
                case e: TimeoutException => warn(e)
              }
            }
          }

          // Stop the zookeeper servers
          val path = config.zk.statusPath
          new EnsurePath(path).ensure(zkCli.getZookeeperClient)
          info("Sending zookeeper termination signal")
          zkCli.setData().forPath(path, "terminate".getBytes)
      }
    }
  }



  @command(description = "list clients in cluster")
  def list {
    val zkServers = defaultZKServers
    if (isAvailable(zkServers)) {
      for((ci, status) <- withZkClient(zkServers) { listServerStatusWith(_) }) {
        val m = ci.m
        println("%s\tCPU:%d\tmemory:%s, pid:%d, status:%s".format(m.host.prefix, m.numCPUs, DataUnit.toHumanReadableFormat(m.memory), ci.pid, status))
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
      error("No zookeeper is running. Run 'silk cluster start' first.")
      return
    }

    withZkClient(z) {
      zkCli =>
        val jvmPID = Shell.getProcessIDOfCurrentJVM
        val ci = getClientInfo(zkCli, hostName)
        if(ci.isEmpty || ci.isDefined && ci.map(_.pid) != jvmPID) {
          val m = MachineResource.thisMachine
          setClientInfo(zkCli, hostName, ClientInfo(m, jvmPID))
          info("Start SilkClient on machine %s", m)
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
      info("Zookeeper is already running at %s", zkHost)
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

  @command(description = "Set loglevel of silk clients")
  def setLogLevel(@argument logLevel: LogLevel) {
    import xerial.silk._
    Silk.hosts foreach { ci =>
      at(ci.m.host) { () =>
        LoggerFactory.setDefaultLogLevel(logLevel)
      }
    }
  }

}