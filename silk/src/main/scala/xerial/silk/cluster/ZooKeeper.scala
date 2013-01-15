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

import org.apache.zookeeper.server.{ZooKeeperServerMain, ServerConfig}
import com.netflix.curator.framework.{CuratorFrameworkFactory, CuratorFramework}
import java.util.Properties
import org.apache.zookeeper.server.quorum.{QuorumPeerMain, QuorumPeerConfig}
import java.io.File
import xerial.core.log.Logger
import com.netflix.curator.CuratorZookeeperClient
import com.netflix.curator.retry.ExponentialBackoffRetry
import io.Source
import com.google.common.io.Files
import com.netflix.curator.framework.state.{ConnectionState, ConnectionStateListener}
import xerial.silk.util.Log4jUtil
import com.netflix.curator.utils.EnsurePath
import xerial.silk.core.SilkSerializer
import org.apache.zookeeper.CreateMode
import xerial.silk.{EmptyZooKeeperConnection, ZookeeperClientIsClosed, SilkException}
import collection.GenTraversableOnce
import collection.generic.{CanBuildFrom, FilterMonadic}


private[cluster] object ZkEnsembleHost {

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
 * Zookeeper ensemble host
 * @param hostName
 * @param quorumPort
 * @param leaderElectionPort
 * @param clientPort
 */
private[cluster] class ZkEnsembleHost(val hostName: String, val quorumPort: Int = config.zk.quorumPort, val leaderElectionPort: Int = config.zk.leaderElectionPort, val clientPort: Int = config.zk.clientPort) {
  override def toString = name
  def clientAddress = "%s:%s".format(hostName, clientPort)
  def name = "%s:%s:%s".format(hostName, quorumPort, leaderElectionPort)
}


object ZkPath {
  implicit def toZkPath(s:String) = ZkPath(s)

  def apply(s:String) : ZkPath = {
    if(!s.startsWith("/"))
      throw new IllegalArgumentException("ZkPath must start with /: %s".format(s))
    else {
      new ZkPath(s.substring(1).split("/").toArray)
    }
  }

  def unapply(s:String) : Option[ZkPath] =
    try
      Some(apply(s))
    catch {
      case e:IllegalArgumentException => None
    }

}


class ZkPath(elems:Array[String]) {
  override def toString = path
  lazy val path = "/" + elems.mkString("/")

  def leaf: String = elems.last

  def / (child:String) : ZkPath = new ZkPath(elems :+ child)
  def parent : Option[ZkPath] = {
    if(elems.length > 1)
      Some(new ZkPath(elems.slice(0, elems.length-1)))
    else
      None
  }
}

trait ZooKeeperClientRef {
  def map[B](f: (ZooKeeperClient) => B) : B
  def flatMap[B](f: (ZooKeeperClient) => B) : Option[B]
  def foreach[U](f: ZooKeeperClient => U) : Unit
}

private[cluster] object EmptyZkClientRef extends ZooKeeperClientRef {
  def map[B](f: (ZooKeeperClient) => B) = throw EmptyZooKeeperConnection
  def flatMap[B](f: (ZooKeeperClient) => B) : Option[B] = None
  def foreach[U](f: ZooKeeperClient => U) : Unit = {}
}

private[cluster] class ZooKeeperClientRefImpl(zk:ZooKeeperClient) extends ZooKeeperClientRef {
  private def wrap[R](f : ZooKeeperClient => R) : R = {
    if(zk.isClosed)
      throw ZookeeperClientIsClosed

    try {
      f(zk)
    }
    finally
      zk.close
  }

  def map[B](f: (ZooKeeperClient) => B) = wrap(f)
  def flatMap[B](f: (ZooKeeperClient) => B) = Some(wrap(f))
  def foreach[U](f: ZooKeeperClient => U) : Unit = wrap(f)
}


/**
 * A simple client for accessing zookeeper
 */
class ZooKeeperClient(cf:CuratorFramework) extends Logger {

  private var closed = false
  def isClosed = closed

  def makePath(zp:ZkPath) {
    new EnsurePath(zp.path).ensure(cf.getZookeeperClient)
  }

  def exists(zp:ZkPath) : Boolean = cf.checkExists.forPath(zp.path) != null

  def ls(zp:ZkPath) : Seq[String] = {
    import collection.JavaConversions._
    cf.getChildren.forPath(zp.path).toSeq
  }

  /**
   * Blocking-read of the path
   * @param zp
   * @return
   */
  def watch(zp:ZkPath) : Array[Byte] = {
    cf.getData.watched().forPath(zp.path)
  }

  def get(zp:ZkPath) : Option[Array[Byte]] = {
    if (exists(zp)) {
      val data = cf.getData.forPath(zp.path)
      Some(data)
    }
    else
      None
  }

  def set(zp:ZkPath, data:Array[Byte], mode:CreateMode=CreateMode.PERSISTENT) {
    // Ensuring the existence of the path
    zp.parent map (makePath)
    if (!exists(zp)) {
      // Write the data to the path
      cf.create().withMode(mode).forPath(zp.path, data)
    }
    cf.setData().forPath(zp.path, data)
  }

  private[cluster] val simpleConnectionListener = new ConnectionStateListener {
    def stateChanged(client: CuratorFramework, newState: ConnectionState) {
      debug("connection state changed: %s", newState.name)
    }
  }

  def start : Unit = {
    cf.getConnectionStateListenable.addListener(simpleConnectionListener)
    cf.start
  }

  def close : Unit = {
    cf.close()
    debug("Closed a zookeeper connection")
    closed = true
  }

  private[cluster] def curatorFramework = cf
}

/**
 * Interface to access ZooKeeper
 *
 * @author leo
 */
object ZooKeeper extends Logger {

  /**
   * Build a zookeeper cluster configuration
   * @param id id in zkHosts
   * @param zkHosts zookeeper hosts
   * @return
   */
  private[cluster] def buildQuorumConfig(id: Int, zkHosts: Seq[ZkEnsembleHost]): QuorumPeerConfig = {

    val isCluster = zkHosts.length > 1

    debug("write myid: %d", id)
    writeMyID(id)

    val properties: Properties = new Properties
    properties.setProperty("tickTime", config.zk.tickTime.toString)
    properties.setProperty("initLimit", config.zk.initLimit.toString)
    properties.setProperty("syncLimit", config.zk.syncLimit.toString)
    val dataDir = config.zkServerDir(id)
    debug("mkdirs: %s", dataDir)
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
   * Write myid file necessary for launching zookeeper ensemble peer
   * @param id
   */
  private[cluster] def writeMyID(id: Int) {
    val myIDFile = config.zkMyIDFile(id)
    xerial.core.io.IOUtil.ensureParentPath(myIDFile)
    if (!myIDFile.exists()) {
      debug("creating myid file at: %s", myIDFile)
      Files.write("%d".format(id).getBytes, myIDFile)
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
      debug("Reading %s", file)
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


  def isAvailable : Boolean = isAvailable(config.zk.getZkServers)

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
    debug("Checking the availability of zookeeper: %s", serverString)
    val available = Log4jUtil.withLogLevel(org.apache.log4j.Level.ERROR) {
      val client = new CuratorZookeeperClient(serverString, 600, 150, null, new ExponentialBackoffRetry(100, 10))
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
      info("No zookeeper is found at %s", serverString)
    else
      info("Found zookeeper: %s", serverString)

    available
  }




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


  private[cluster] def defaultZkClient : ZooKeeperClientRef  = zkClient(config.zk.zkServersConnectString)
  private[cluster] def zkClient(zkConnectString:String) : ZooKeeperClientRef = {
    if(isAvailable(zkConnectString)) {
      val c = new ZooKeeperClient(CuratorFrameworkFactory.newClient(zkConnectString, new ExponentialBackoffRetry(300, 10)))
      c.start
      new ZooKeeperClientRefImpl(c)
    }
    else {
      error("No zookeeper appears to be running at %s. Run 'silk cluster start' first.", zkConnectString)
      EmptyZkClientRef
    }
  }

  private def withZkClient[U](f:ZooKeeperClient => U) : U =
    withZkClient(config.zk.getZkServers)(f)


  private def withZkClient[U](zkServers: Seq[ZkEnsembleHost])(f: ZooKeeperClient => U): U =
    withZkClient(zkServers.map(_.clientAddress).mkString(","))(f)

  private def withZkClient[U](zkServerAddr: String)(f: ZooKeeperClient => U): U = {
    val c = new ZooKeeperClient(CuratorFrameworkFactory.newClient(zkServerAddr, new ExponentialBackoffRetry(300, 10)))
    c.start
    try {
      f(c)
    }
    finally {
      c.close
    }
  }

}
