//--------------------------------------
//
// ClusterCommand.scala
// Since: 2012/12/19 11:54 AM
//
//--------------------------------------

package xerial.silk.cluster

import xerial.silk.DefaultMessage
import xerial.core.log.Logger
import com.netflix.curator.test.ByteCodeRewrite
import xerial.lens.cui.{argument, option, command}
import xerial.core.util.{DataUnit, Shell}
import com.netflix.curator.utils.EnsurePath
import xerial.silk.core.SilkSerializer
import org.apache.zookeeper.CreateMode
import java.util.concurrent.{TimeUnit, Executors}
import xerial.silk.cluster.SilkClient.Terminate

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