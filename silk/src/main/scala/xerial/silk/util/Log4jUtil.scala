//--------------------------------------
//
// Log4jUtil.scala
// Since: 2012/12/18 11:50 AM
//
//--------------------------------------

package xerial.silk.util

/**
 * @author Taro L. Saito
 */
object Log4jUtil {

  def withLogLevel[U](level:org.apache.log4j.Level)(f: => U) : U = {
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