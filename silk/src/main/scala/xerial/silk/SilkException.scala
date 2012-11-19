//--------------------------------------
//
// SilkException.scala
// Since: 2012/11/19 2:08 PM
//
//--------------------------------------

package xerial.silk

import xerial.core.util.CName

/**
 * @author leo
 */
abstract class SilkException(private val message:String) extends Exception(message) with SilkExceptionBase {
}

trait SilkExceptionBase {
  def errorCode = CName.toNaturalName(this.getClass.getSimpleName).toUpperCase

  override def toString = {
    "[%s] %s".format(errorCode, super.toString)
  }
}


abstract class SilkError(private val message:String) extends Error(message) with SilkExceptionBase {
}

case class InvalidFormat(message:String) extends SilkException(message)
case class ParseError(line:Int, pos:Int, message:String)
  extends SilkException("(line:%d, pos:%d) %s".format(line, pos, message))

