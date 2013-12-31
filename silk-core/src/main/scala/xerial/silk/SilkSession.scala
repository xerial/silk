//--------------------------------------
//
// SilkSession.scala
// Since: 2013/12/31 15:34
//
//--------------------------------------

package xerial.silk

import java.util.UUID
import java.nio.charset.Charset


object SilkSession {

  def defaultSession = new SilkSession("default")
}


/**
 * Session is a reference to the computed result of a Silk operation.
 */
case class SilkSession(id:UUID, name:String) {
  def this(name:String) = this(UUID.nameUUIDFromBytes(name.getBytes(Charset.forName("UTF8"))), name)

}