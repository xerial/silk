//--------------------------------------
//
// SilkMaster.scala
// Since: 2013/01/08 10:15 AM
//
//--------------------------------------

package xerial.silk.cluster

import akka.actor.Actor
import java.util.UUID
import xerial.core.log.Logger

object SilkMaster {
  /**
   * Master messages
   */

  case class RegisterClassBox(cb:ClassBox, holder:ClientAddr)
  case class AskClassBoxHolder(id:String)
  case class ClassBoxHolder(cb:ClassBox, holder:ClientAddr)
  case class ClassBoxNotFound(id:String)
}


/**
 * @author Taro L. Saito
 */
class SilkMaster extends Actor with Logger {

  import SilkMaster._

  private val classBoxLocation = scala.collection.mutable.Map[String, Set[ClientAddr]]()
  private val classBoxTable = scala.collection.mutable.Map[String, ClassBox]()


  override def preStart() {
    info("Start SilkMaster at %s:%s", localhost.address, config.silkMasterPort)
  }

  protected def receive = {
    case RegisterClassBox(cb, holder) =>
      info("Registering a ClassBox: %s", cb.id)
      classBoxTable.getOrElseUpdate(cb.id, cb)
      val prevHolders : Set[ClientAddr] = classBoxLocation.getOrElseUpdate(cb.id, Set())
      classBoxLocation += cb.id -> (prevHolders + holder)
    case AskClassBoxHolder(id) =>
      if(classBoxLocation.contains(id)) {
        val holder = classBoxLocation(id)
        // TODO return a closest or free holder address
        sender ! ClassBoxHolder(classBoxTable(id), holder.head)
      }
      else {
        sender ! ClassBoxNotFound(id)
      }


  }
  override def postStop() {
    info("Stopped SilkMaster")
  }
}