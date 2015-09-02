//--------------------------------------
//
// Log4jUtil.scala
// Since: 2012/12/18 11:50 AM
//
//--------------------------------------

package xerial.silk.core.util

import org.apache.log4j.{EnhancedPatternLayout, Appender, BasicConfigurator, Level}

/**
 * @author Taro L. Saito
 */
object Log4jUtil {

  def configureLog4j {
    configureLog4jWithLogLevel(Level.WARN)
  }

  def suppressLog4jwarning {
    configureLog4jWithLogLevel(Level.ERROR)
  }

  def configureLog4jWithLogLevel(level: org.apache.log4j.Level) {
    BasicConfigurator.configure
    val rootLogger = org.apache.log4j.Logger.getRootLogger
    rootLogger.setLevel(level)
    val it = rootLogger.getAllAppenders
    while (it.hasMoreElements) {
      val a = it.nextElement().asInstanceOf[Appender]
      a.setLayout(new EnhancedPatternLayout("[%t] %p %c{1} - %m%n%throwable"))
    }
  }


  def withLogLevel[U](level: org.apache.log4j.Level)(f: => U): U = {
    val r = org.apache.log4j.Logger.getRootLogger
    val prev = r.getLevel
    r.setLevel(level)
    try {
      f
    }
    finally {
      r.setLevel(prev)
    }
  }

}