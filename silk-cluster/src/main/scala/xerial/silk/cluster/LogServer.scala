//--------------------------------------
//
// LogServer.scala
// Since: 2013/01/11 2:22 PM
//
//--------------------------------------

package xerial.silk.cluster

import akka.actor.Actor
import xerial.core.log.LogLevel
import xerial.silk.framework.Host


case class SilkLog(host:Host, logLevel:LogLevel, message:String)

/**
 * An actor aggregating log ouputs
 *
 * @author Taro L. Saito
 */
class LogServer extends Actor {
  def receive = {
    case l:SilkLog =>
  }
}