package xerial

import scala.language.experimental.macros
import scala.language.implicitConversions
import xerial.silk.framework.ops._
import scala.reflect.ClassTag
import xerial.silk.framework.WorkflowMacros
import org.apache.log4j.{EnhancedPatternLayout, Appender, BasicConfigurator, Level}

/**
 * Helper methods for using Silk. Import this package as follows:
 *
 * {{{
 *   import xerial.silk._
 * }}}
 *
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
      a.setLayout(new EnhancedPatternLayout("[%t] %p %c{1} - %m%n%throwable"))
    }
  }


  implicit class SilkSeqWrap[A](val a:Seq[A]) {
    def toSilk(implicit ev:ClassTag[A]) : SilkSeq[A] = macro SilkMacros.mRawSmallSeq[A]
  }

  implicit class SilkArrayWrap[A:ClassTag](a:Array[A]) {
    def toSilk : SilkSeq[A] = SilkException.NA
  }

  implicit class SilkWrap[A:ClassTag](a:A) {
    def toSilkSingle : SilkSingle[A] = SilkException.NA
  }

  implicit class CommandBuilder(val sc:StringContext) extends AnyVal {
    def c(args:Any*) : CommandOp = macro SilkMacros.mCommand
  }

  /**
   * Import another workflow trait as a mixin to this class. The imported workflow shares the same session
   * @param ev
   * @tparam A
   * @return
   */
  def mixin[A](implicit ev:ClassTag[A]) : A = macro WorkflowMacros.mixinImpl[A]



}
