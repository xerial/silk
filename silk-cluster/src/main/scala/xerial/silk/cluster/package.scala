package xerial.silk

import java.io.File
import java.net.InetAddress
import xerial.core.log.Logger
import scala.util.DynamicVariable
import com.netflix.curator.test.ByteCodeRewrite
import org.apache.log4j._
import xerial.silk.framework.{NodeRef, NodeResource, Node, Host}
import xerial.silk.framework.NodeRef
import xerial.silk.framework.Node

/**
 * Cluster configuration parameters
 *
 * @author Taro L. Saito
 */
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



  def hosts : Seq[Node] = {
    val ci = ZooKeeper.defaultZkClient.flatMap(zk => ClusterCommand.collectClientInfo(zk))
    ci.toSeq
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
  private var _config : Config = Config()

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



}
