//--------------------------------------
//
// SilkCluster.scala
// Since: 2013/11/14 23:35
//
//--------------------------------------

package xerial.silk.cluster

import xerial.silk.cluster.store.DistributedCache
import xerial.silk.cluster.rm.ClusterNodeManager
import xerial.silk.framework.{NodeRef, Host, Node}
import java.net.{UnknownHostException, InetAddress}
import xerial.silk.{Silk, SilkEnv}
import scala.io.Source
import java.io.File
import xerial.core.log.Logger
import xerial.silk.util.Guard

/**
 * @author Taro L. Saito
 */
object SilkCluster extends Guard with Logger {

  def hosts : Seq[Node] = {

    def collectClientInfo(zkc: ZooKeeperClient): Seq[Node] = {
      val cm = new ClusterNodeManager with ZooKeeperService {
        val zk = zkc
      }
      cm.nodeManager.nodes
    }

    val ci = ZooKeeper.defaultZkClient.flatMap(zk => collectClientInfo(zk))
    ci.toSeq
  }

  def master : Option[MasterRecord] = {
    def getMasterInfo(zkc: ZooKeeperClient) : Option[MasterRecord] = {
      val cm = new MasterRecordComponent  with ZooKeeperService with DistributedCache {
        val zk = zkc
      }
      cm.getMaster
    }
    ZooKeeper.defaultZkClient.flatMap(zk => getMasterInfo(zk)).headOption
  }



  private var silkEnvList : List[SilkInitializer] = List.empty

  /**
   * Initialize a Silk environment
   * @param zkConnectString
   * @return
   */
  def init(zkConnectString: => String = config.zk.zkServersConnectString) = {
    val launcher = new SilkInitializer(zkConnectString)
    // Register a new launcher
    guard {
      silkEnvList ::= launcher
    }
    launcher.start
  }


  /**
   * Clean up all SilkEnv
   */
  def cleanUp = guard {
    for(env <- silkEnvList.par)
      env.stop
    silkEnvList = List.empty
  }

  def defaultHosts(clusterFile:File = config.silkHosts): Seq[Host] = {
    if (clusterFile.exists()) {
      def getHost(line: String): Option[Host] = {
        try
          Host.parseHostsLine(line)
        catch {
          case e: UnknownHostException => {
            warn(s"unknown host: $line")
            None
          }
        }
      }
      val hosts = for {
        (line, i) <- Source.fromFile(clusterFile).getLines.zipWithIndex
        host <- getHost(line)
      } yield host
      hosts.toSeq
    }
    else {
      warn("$HOME/.silk/hosts is not found. Use localhost only")
      Seq(localhost)
    }
  }



  /**
   * Execute a command at the specified host
   * @param h
   * @param f
   * @tparam R
   * @return
   */
  def at[R](h:Host)(f: => R)(implicit env:SilkEnv) : R = {
    Remote.at[R](NodeRef(h.name, h.address, config.silkClientPort))(f)
  }

  def at[R](n:Node)(f: => R)(implicit env:SilkEnv) : R =
    Remote.at[R](n.toRef)(f)

  def at[R](n:NodeRef)(f: => R)(implicit env:SilkEnv) : R =
    Remote.at[R](n)(f)


  private var _localhost : Host = {
    try {
      val lh = InetAddress.getLocalHost
      val addr = System.getProperty("silk.localaddr", lh.getHostAddress)
      Host(lh.getHostName, addr)
    }
    catch {
      case e:UnknownHostException =>
        val addr = InetAddress.getLoopbackAddress
        Host(addr.getHostName, addr.getHostAddress)
    }
  }

  def setLocalHost(h:Host) { _localhost = h }

  def localhost : Host = _localhost

}