package xerial

import silk.cluster.{Remote, Host}
import silk.core.SilkWorkflow.{SilkFlowSingle, SilkFile, CommandSeq, ShellCommand}
import xerial.silk.core._
import java.io.File
import org.apache.log4j.{Level, PatternLayout, Appender, BasicConfigurator}
import xerial.silk.core.CmdString
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


  import scala.language.implicitConversions
  implicit def toSilkCmd(cmd:CmdString) = ShellCommand(cmd)

  implicit class SilkCommandWrap(cmd:CmdString) {
    def toFile(name:String): SilkSingle[File] = { // TODO
      Silk.single(new File(name))
    }

    def ==>(files:Seq[String]) : Silk[File] = SilkInMemory(files.map(new File(_)))

    def !! : Silk[String] = ShellCommand(cmd)
    def toSilk : Silk[String] = ShellCommand(cmd)
    def lines : Silk[String] = ShellCommand(cmd)
    def file : SilkFile = SilkFile(cmd) // TODO impl
    def &&[A](next:A) : SilkFlowSingle[Nothing, A] = CommandSeq(ShellCommand(cmd), Silk.single(next))
    def |[A,B](next:A => B) = SilkCommandWrap(cmd) // TODO fixme
  }


  //implicit def wrapAsSilkSeq[A](a:Array[A]) = new SilkSeqWrap(a)

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
    def c(args:Any*) : CmdString = new CmdString(sc, args:_*)
  }


}
