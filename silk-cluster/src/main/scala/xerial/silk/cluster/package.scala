package xerial.silk

import java.io.File
import java.net.InetAddress
import xerial.core.log.Logger
import scala.util.DynamicVariable
import com.netflix.curator.test.ByteCodeRewrite
import xerial.silk.cluster.SilkClient.ClientInfo
import org.apache.log4j.{Level, PatternLayout, Appender, BasicConfigurator}

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
      a.setLayout(new PatternLayout("[%t] %p %c{1} %x - %m%n"))
    }
  }



  def hosts : Seq[ClientInfo] = {
    val ci = ZooKeeper.defaultZkClient.flatMap(zk => ClusterCommand.collectClientInfo(zk))
    ci getOrElse Seq.empty
  }

  /**
   * Execute a command at the specified host
   * @param h
   * @param f
   * @tparam R
   * @return
   */
  def at[R](h:Host)(f: => R) : R = {
    // TODO fixme
    Remote.at[R](ClientInfo(h, config.silkClientPort, config.dataServerPort, null, -1))(f)
  }

  def at[R](cli:ClientInfo)(f: => R) : R =
    Remote.at[R](cli)(f)


  val localhost: Host = {
    val lh = InetAddress.getLocalHost
    Host(lh.getHostName, lh.getHostAddress)
  }


  // TODO setting configurations from SILK_CONFIG file
  /**
   * A global variable for accessing the configurations using `config.get`.
   *
   * TODO: This value should be shared between thread, rather than thread-local storage
   */

  private var _config : Config = Config()

  def config = _config

  /**
   * Switch the configurations
   * @param c
   * @param f
   * @tparam U
   * @return
   */
  def withConfig[U](c:Config)(f: => U) : U = {
    info(s"Switching the configuration: $c")
    val prev = _config
    try {
      _config = c
      f
    }
    finally
     _config = c
  }



}
