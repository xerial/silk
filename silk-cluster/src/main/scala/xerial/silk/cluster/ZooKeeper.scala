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
import com.netflix.curator.utils.{ZKPaths, EnsurePath}
import org.apache.zookeeper.{KeeperException, CreateMode}
import xerial.silk.{ZookeeperClientIsClosed, SilkException}
import xerial.silk.framework.Host
import xerial.silk.io.{MissingService, ServiceGuard}


private[silk] object ZkEnsembleHost {

  def apply(s: String): ZkEnsembleHost = {
    val c = s.split(":")
    c.length match {
      case 2 => // host:(client port)
        new ZkEnsembleHost(Host(c(0)), clientPort=c(1).toInt)
      case 3 => // host:(quorum port):(leader election port)
        new ZkEnsembleHost(Host(c(0)), c(1).toInt, c(2).toInt)
      case _ => // hostname only
        new ZkEnsembleHost(Host(s))
    }
  }

  def unapply(s: String): Option[ZkEnsembleHost] = {
    try
      Some(apply(s))
    catch {
      case e : Exception => None
    }
  }
}


/**
 * Zookeeper ensemble host
 * @param host
 * @param quorumPort
 * @param leaderElectionPort
 * @param clientPort
 */
private[silk] class ZkEnsembleHost(val host: Host, val quorumPort: Int = config.zk.quorumPort, val leaderElectionPort: Int = config.zk.leaderElectionPort, val clientPort: Int = config.zk.clientPort) {
  override def toString = connectAddress
  def connectAddress = "%s:%s".format(host.address, clientPort)
  def serverAddress = "%s:%s:%s".format(host.prefix, quorumPort, leaderElectionPort)
}





/**
 * A simple client for accessing zookeeper
 */
class ZooKeeperClient(cf:CuratorFramework) extends Logger  {

  private val pathCache = collection.mutable.WeakHashMap[ZkPath, EnsurePath]()

  private var closed = false
  def isClosed = closed

  def ensureOpen {
    require(!closed, "ZooKeeperConnection is closed")
  }

  def ensurePath(zp:ZkPath) {
    ensureOpen
    // Reuse EnsurePath instances so that second operation for the same operation will be no-op
    val helper = pathCache.getOrElseUpdate(zp, new EnsurePath(zp.path))
    helper.ensure(cf.getZookeeperClient)
  }

  def exists(zp:ZkPath) : Boolean = {
    ensureOpen
    cf.checkExists.forPath(zp.path) != null
  }

  def ls(zp:String) : Seq[String] = ls(ZkPath(zp))

  def ls(zp:ZkPath) : Seq[String] = {
    ensureOpen
    import collection.JavaConversions._
    try {
      cf.getChildren.forPath(zp.path).toSeq
    }
    catch {
      case e:KeeperException.NoNodeException =>
        warn(e.getMessage)
        Seq.empty
    }
  }

  /**
   * Blocking-read of the path
   * @param zp
   * @return
   */
  def watch(zp:ZkPath) : Array[Byte] = {
    ensureOpen
    cf.getData.watched().forPath(zp.path)
  }

//  /**
//   * Get and watch the data. If no node exists for the path, create the path and initialize it with null
//   * @param zp
//   * @param watcher
//   * @return
//   */
//  def getAndWatch(zp:ZkPath, watcher:CuratorWatcher): Option[Array[Byte]] = {
//    try {
//      ensurePath(zp)
//      Some(cf.getData.usingWatcher(watcher).forPath(zp.path))
//    }
//    catch {
//      case e:NoNodeException => None
//    }
//  }


  def read(zp:ZkPath) : Array[Byte] = {
    ensureOpen
    cf.getData.forPath(zp.path)
  }
  def get(zp:String) : Option[Array[Byte]] = get(ZkPath(zp))
  def get(zp:ZkPath) : Option[Array[Byte]] = {
    if (exists(zp)) {
      val data = cf.getData.forPath(zp.path)
      Some(data)
    }
    else
      None
  }

  def set(zp:ZkPath, data:Array[Byte], mode:CreateMode=CreateMode.PERSISTENT) {
    ensureOpen
    // Ensuring the existence of the path
    zp.parent map (ensurePath)
    if (!exists(zp)) {
      // Write the data to the path
      cf.create().withMode(mode).forPath(zp.path, data)
    }
    else
      cf.setData().forPath(zp.path, data)
  }

  def remove(zp:ZkPath) {
    ensureOpen
    if(exists(zp)) {
       cf.delete().forPath(zp.path)
    }
  }

  private[cluster] val simpleConnectionListener = new ConnectionStateListener {
    def stateChanged(client: CuratorFramework, newState: ConnectionState) {
      newState match {
        case ConnectionState.LOST =>
          trace("Connection to ZooKeeper is lost")
          closed=true
        case ConnectionState.SUSPENDED =>
          trace("Connection to ZooKeeper is suspended")
        case _ =>
          trace(s"Connection state has changed: $newState")
      }
    }
  }

  def start : Unit = {
    cf.getConnectionStateListenable.addListener(simpleConnectionListener)
    cf.start
    trace(f"Started a new zookeeper connection: ${this.hashCode()}%08x")

  }

  def close : Unit = {
    if(!closed) {
      cf.close()
      trace(f"Closed a zookeeper connection: ${this.hashCode()}%08x")
    }
    closed = true
  }

  private[silk] def curatorFramework = cf

}


object ZkPath {
  //implicit def toZkPath(s:String) = ZkPath(s)

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

  def / (child:String) : ZkPath = new ZkPath(elems ++ child.split("/"))
  def parent : Option[ZkPath] = {
    if(elems.length > 1)
      Some(new ZkPath(elems.slice(0, elems.length-1)))
    else
      None
  }
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
  private[silk] def buildQuorumConfig(id: Int, zkHosts: Seq[ZkEnsembleHost]): QuorumPeerConfig = {

    val isCluster = zkHosts.length > 1

    debug(s"write myid: $id")
    writeMyID(id)

    val properties: Properties = new Properties
    properties.setProperty("tickTime", config.zk.tickTime.toString)
    properties.setProperty("initLimit", config.zk.initLimit.toString)
    properties.setProperty("syncLimit", config.zk.syncLimit.toString)
    val dataDir = config.zkServerDir(id)
    debug(s"mkdirs: $dataDir")
    dataDir.mkdirs()

    properties.setProperty("dataDir", dataDir.getCanonicalPath)
    properties.setProperty("clientPort", config.zk.clientPort.toString)
    if (isCluster) {
      for ((h, hid) <- zkHosts.zipWithIndex) {
        val serverString = h.serverAddress
        debug(s"zk server address: $serverString")
        properties.setProperty("server." + hid, serverString)
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
      debug(s"creating myid file at: $myIDFile")
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
      debug(s"file $file not found")
      None
    }
    else {
      debug(s"Reading $file")
      val r = for {
        (l, i) <- Source.fromFile(file).getLines().toSeq.zipWithIndex
        h <- l.trim match {
          case z if z.startsWith("#") => None // comment line
          case ZkEnsembleHost(z) => Some(z)
          case _ =>
            warn(s"invalid line (${i+1}) in $file: $l")
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
  def isAvailable(servers: Seq[ZkEnsembleHost]): Boolean = isAvailable(servers.map(_.connectAddress).mkString(","))

  /**
   * Check the availability of the zookeeper servers
   * @param serverString comma separated list of (addr):(zkClientPort)
   * @return
   */
  def isAvailable(serverString: String): Boolean =  {
    // Try to connect the ZooKeeper ensemble using a short delay
    debug(s"Checking the availability of zookeeper: $serverString")
    val available = Log4jUtil.withLogLevel(org.apache.log4j.Level.ERROR) {
      val client = new CuratorZookeeperClient(serverString, 6000, 3000, null, retryPolicy)
      try {
        client.start
        client.blockUntilConnectedOrTimedOut()
      }
      catch {
        case e: Throwable =>
          false
      }
      finally {
        client.close
      }
    }

    if (!available)
      debug(s"No zookeeper is found at $serverString")
    else
      debug(s"Found zookeeper: $serverString")

    available
  }



  /**
   * An interface for launching zookeeper
   */
  private[silk] trait ZkServer {
    def run(config: QuorumPeerConfig): Unit
    def shutdown: Unit
  }

  /**
   * An instance of a clustered zookeeper
   */
  private[silk] class ZkQuorumPeer extends QuorumPeerMain with ZkServer {
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
  private[silk] class ZkStandalone extends ZooKeeperServerMain with ZkServer {
    def run(config: QuorumPeerConfig): Unit = {
      val sConfig = new ServerConfig
      sConfig.readFrom(config)
      runFromConfig(sConfig)
    }
    override def shutdown {
      super.shutdown
    }
  }

  /**
   * Get a ZooKeeper client. It will retry connection to the server the number of times specified by config.zk.clientConnectionMaxRetry.
   *
   * @return connection wrapper that can be used in for-comprehension
   */
  def defaultZkClient : ServiceGuard[ZooKeeperClient]  = zkClient(config.zk.zkServersConnectString)
  def zkClient(zkConnectString:String) : ServiceGuard[ZooKeeperClient] = {
    try {
      val cf = CuratorFrameworkFactory.newClient(zkConnectString, config.zk.clientSessionTimeout, config.zk.clientConnectionTimeout, retryPolicy)
      val c = new ZooKeeperClient(cf)
      c.start
      new ServiceGuard[ZooKeeperClient] {
        protected[silk] val service = c
        def close { c.close }
      }
    }
    catch {
      case e : Exception =>
        error(e)
        new MissingService[ZooKeeperClient]()
    }
  }

  private def retryPolicy = {
    new ExponentialBackoffRetry(config.zk.clientConnectionTickTime, config.zk.clientConnectionMaxRetry)
  }

}
