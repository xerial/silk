package xerial

import core.log.Logger
import silk.cluster.SilkClient.ClientInfo
import silk.cluster.{ZooKeeper, ClusterCommand, Remote, Host}
import silk.flow.{Silk}
import .CommandSeq
import .ShellCommand
import .SilkFile
import java.io.File
import org.apache.log4j.{Level, PatternLayout, Appender, BasicConfigurator}
import xerial.silk.cluster.SilkClient.ClientInfo


/**
 * @author Taro L. Saito
 */
package object silk {

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

  def fromFile[A](path:String) = new SilkFileSource(path)

  def toSilk[A](obj: A): Silk[A] = {
    new SilkInMemory[A](Seq(obj))
  }

  def toSilkSeq[A](a:Seq[A]) : Silk[A] = {
    new SilkInMemory(a)
  }

  def toSilkArray[A](a:Array[A]) : Silk[A] = {
    // TODO optimization
    new SilkInMemory(a.toSeq)
  }


  implicit class SilkWrap[A](a:A) {
    def toSilk : Silk[A] = null // TODO impl
    def save = {
      // do something to store Silk data
    }
  }

  implicit class SilkArrayWrap[A](a:Array[A]) {
    def toSilk : Silk[A] = {
      // TODO impl
      null
    }
  }

  implicit class SilkSeqWrap[A](a:Seq[A]) {
    def toSilk : Silk[A] = SilkInMemory[A](a)
    def toFlow(name:String) : Silk[A] = SilkWorkflow.newWorkflow(name, a.toSilk)
  }

  /**
   * Execute a command at the specified host
   * @param h
   * @param f
   * @tparam R
   * @return
   */
  def at[R](h:Host)(f: => R) : R = {
    import cluster.config
    // TODO fixme
    Remote.at[R](ClientInfo(h, config.silkClientPort, config.dataServerPort, null, -1))(f)
  }

  def at[R](cli:ClientInfo)(f: => R) : R =
    Remote.at[R](cli)(f)


  implicit class CmdBuilder(val sc:StringContext) extends AnyVal {
    def c(args:Any*) : ShellCommand = {
      new ShellCommand(sc, args:_*)
    }
  }


}
