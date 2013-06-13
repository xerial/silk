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
// ClusterManager.scala
// Since: 2012/12/13 3:15 PM
//
//--------------------------------------

package xerial.silk.cluster

import java.io.File
import io.Source
import java.net.{UnknownHostException, InetAddress}
import xerial.core.log.Logger
import xerial.core.util.Shell
import xerial.lens.cui.command
import xerial.silk
import com.netflix.curator.framework.CuratorFramework
import xerial.silk.cluster.SilkClient.{ReportStatus}
import com.netflix.curator.utils.EnsurePath
import xerial.silk.core.SilkSerializer
import java.util.concurrent.TimeoutException
import org.apache.zookeeper.CreateMode
import xerial.silk.framework.{Host, ClusterManagerComponent}

/**
 * @author Taro L. Saito
 */
object ClusterManager extends Logger {


  def defaultHosts(clusterFile:File = config.silkHosts): Seq[Host] = {
    if (clusterFile.exists()) {
      def getHost(hostname: String): Option[Host] = {
        try {
          val addr = InetAddress.getByName(hostname)
          Some(Host(hostname, addr.getHostAddress))
        }
        catch {
          case e: UnknownHostException => {
            warn(s"unknown host: $hostname")
            None
          }
        }
      }
      val hosts = for {
        (line, i) <- Source.fromFile(clusterFile).getLines.zipWithIndex
        host = line.trim
        if !host.isEmpty && !host.startsWith("#")
        h <- getHost(host)
      } yield h
      hosts.toSeq
    }
    else {
      warn("$HOME/.silk/hosts is not found. Use localhost only")
      Seq(localhost)
    }
  }

  /**
   * Check wheather silk is installed
   * @param h
   */
  def isSilkInstalled(h:Host) : Boolean = {
    val ret = Shell.exec("ssh -n %s '$SHELL -l -c silk version'".format(h.name))
    ret == 0
  }


}


//class ClusterManager extends ClusterManagerComponent {
//  type Node = ClientAddr
//
//
//  type NodeManager = NodeManagerImpl
//  val nodeManager = new NodeManagerImpl
//
//  class NodeManagerImpl extends NodeManagerAPI {
//    def nodes = ???
//    def addNode(n: Node) {}
//    def removeNode(n: Node) {}
//  }
//}
