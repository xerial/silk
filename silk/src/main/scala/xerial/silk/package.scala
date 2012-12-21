package xerial

import silk.cluster.{Remote, Host}
import silk.core.{SilkInMemory, Silk}
import java.io.File
import org.apache.log4j.{Level, PatternLayout, Appender, BasicConfigurator}


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


  val SILK_HOME : File = {
    val homeDir = sys.props.get("user.home") getOrElse ("")
    new File(homeDir, ".silk")
  }



  class SilkWrap[A](a:A) {
    def save = {
      // do something to store Silk data
    }
  }

  class SilkArrayWrap[A](a:Array[A]) {
    def toSilk : Silk[A] = {
      // TODO impl
      null
    }
  }

  class SilkSeqWrap[A](a:Seq[A]) {
    def toSilk : Silk[A] = SilkInMemory[A](a)
  }



  implicit def wrapAsSilk[A](a:A) = new SilkWrap(a)
  implicit def wrapAsSilkArray[A](a:Array[A]) = new SilkArrayWrap(a)
  implicit def asSilkSeq[A](a:Seq[A]) = new SilkSeqWrap(a)
  //implicit def wrapAsSilkSeq[A](a:Array[A]) = new SilkSeqWrap(a)

//
//  /**
//   * Execute command at the specified host
//   * @param h
//   * @param f
//   * @tparam B
//   * @return
//   */
//  def at[A, B](h:Host)(f: A => B) : B = {
//    Remote.at(h)(f)
//  }

  def at[R](h:Host)(f: Function0[R]) : R = {
    Remote.at[R](h)(f)
  }


}
