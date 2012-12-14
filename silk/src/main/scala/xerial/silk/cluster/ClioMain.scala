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
// ClioMain.scala
// Since: 2012/10/24 3:00 PM
//
//--------------------------------------

package xerial.silk.cluster

import xerial.lens.cui.{CommandModule, command, option}
import io.Source
import xerial.core.log.Logger
import java.io.File
import com.netflix.curator.framework.CuratorFrameworkFactory
import com.netflix.curator.retry.ExponentialBackoffRetry
import com.netflix.curator.CuratorZookeeperClient
import org.apache.log4j.BasicConfigurator
import xerial.silk.DefaultMessage


class ZkEnsembleHost(val hostName: String, val quorumPort: Int = 2888, val leaderElectionPort: Int = 3888) {
  def serverName = "%s:%s".format(hostName, quorumPort)
  def name = "%s:%s:%s".format(hostName, quorumPort, leaderElectionPort)
}

object ZkEnsembleHost extends Logger {
  def unapply(s: String): Option[ZkEnsembleHost] = {
    val c = s.split(":")
    try {
      val h = c.length match {
        case 2 => // host:(quorum port)
          new ZkEnsembleHost(c(0), c(1).toInt)
        case 3 => // host:(quorum port):(leader election port)
          new ZkEnsembleHost(c(0), c(1).toInt, c(2).toInt)
        case _ => // hostname only
          new ZkEnsembleHost(s)
      }
      Some(h)
    }
    catch {
      case e => None
    }
  }
}

object ClusterCommand extends Logger {


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
      info("randomly pick up servers from $HOME/.silk/hosts")
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
    ensembleServers
  }


}

/**
 * @author leo
 */
class ClusterCommand extends DefaultMessage with Logger {

  import ClusterCommand._

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

    // Find ZK server



    // Read hosts file

    // Launch SilkClients on each host


  }

  @command(description="Shut down silk cluster")
  def stop {
    // Find ZK server

    // Get Akka Actor Addresses of SilkClient

    // Send Termination signal


  }

}