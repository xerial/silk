

package xerial.silk

import com.netflix.curator.test.ByteCodeRewrite
import org.apache.log4j.{EnhancedPatternLayout, Appender, BasicConfigurator, Level}
import xerial.silk.framework.{NodeRef, Host, Node}
import xerial.silk.cluster.{Remote, Config, ZooKeeper}
import java.net.InetAddress
import xerial.core.log.Logger
import xerial.silk.cluster.framework.{MasterRecord, MasterRecordComponent, ZooKeeperService, ClusterNodeManager}
import scala.io.Source
import java.rmi.UnknownHostException
import java.io.File


package object cluster extends Logger {

  /**
   * This code is a fix for MXBean unregister problem: https://github.com/Netflix/curator/issues/121
   */
  ByteCodeRewrite.apply()

  //xerial.silk.suppressLog4jwarning

  def configureLog4j {
    configureLog4jWithLogLevel(Level.WARN)
  }

  def suppressLog4jwarning {
    configureLog4jWithLogLevel(Level.ERROR)
  }

  def configureLog4jWithLogLevel(level:org.apache.log4j.Level){
    BasicConfigurator.configure
    val rootLogger = org.apache.log4j.Logger.getRootLogger
    rootLogger.setLevel(level)
    val it = rootLogger.getAllAppenders
    while(it.hasMoreElements) {
      val a = it.nextElement().asInstanceOf[Appender]
      a.setLayout(new EnhancedPatternLayout("[%t] %p %c{1} - %m%n%throwable"))
    }
  }


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
      val cm = new MasterRecordComponent  with ZooKeeperService {
        val zk = zkc
      }
      cm.getMaster
    }
    ZooKeeper.defaultZkClient.flatMap(zk => getMasterInfo(zk)).headOption
  }

  /**
   * Execute a command at the specified host
   * @param h
   * @param f
   * @tparam R
   * @return
   */
  def at[R](h:Host)(f: => R) : R = {
    Remote.at[R](NodeRef(h.name, h.address, config.silkClientPort))(f)
  }

  def at[R](n:Node)(f: => R) : R =
    Remote.at[R](n.toRef)(f)

  def at[R](n:NodeRef)(f: => R) : R =
    Remote.at[R](n)(f)


  val localhost: Host = {
    val lh = InetAddress.getLocalHost
    Host(lh.getHostName, lh.getHostAddress)
  }


  // TODO setting configurations from SILK_CONFIG file
  /**
   * A global variable for accessing the configurations using `config.get`.
   *
   * This value is shared between thread rather than stored in thread-local storage
   */
  @volatile private var _config : Config = Config()

  def config = _config

  /**
   * Switch the configurations within the given function block
   * @param c
   * @param f
   * @tparam U
   * @return
   */
  def withConfig[U](c:Config)(f: => U) : U = {
    debug(s"Switch the configuration: $c")
    val prev = _config
    try {
      _config = c
      f
    }
    finally
      _config = prev
  }


  def silkEnv[U](zkConnectString:String)(body: => U) : U = {
    withConfig(Config.testConfig(zkConnectString)) {
      SilkEnvImpl.silk(body)
    }
  }


}


