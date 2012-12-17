package xerial

import silk.core.{SilkInMemory, Silk}
import java.io.File
import org.apache.log4j.{PatternLayout, Appender, BasicConfigurator}

/**
 * @author Taro L. Saito
 */
package object silk {

  def configureLog4j {
    BasicConfigurator.configure
    val rootLogger = org.apache.log4j.Logger.getRootLogger
    rootLogger.setLevel(org.apache.log4j.Level.WARN)
    val it = rootLogger.getAllAppenders
    while(it.hasMoreElements) {
      val a = it.nextElement().asInstanceOf[Appender]
      a.setLayout(new PatternLayout("[%t] %p %c{1} %x - %m%n"))
    }
  }


  val silkHome : File = {
    val homeDir = sys.props.get("user.home") getOrElse ("")
    new File(homeDir + "/.silk")
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


}
