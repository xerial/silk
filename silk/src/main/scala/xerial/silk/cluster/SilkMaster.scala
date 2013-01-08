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
   * @param uuid
   * @param holder
   */
  case class RegisterClassBox(uuid:UUID, holder:ClientAddr)
  case class AskClassBoxHolder(uuid:UUID)
  case class ClassBoxHolder(uuid:UUID, holder:ClientAddr)
  case class ClassBoxNotFound(uuid:UUID)
}


/**
 * @author Taro L. Saito
 */
class SilkMaster extends Actor with Logger {

  import SilkMaster._

  private val classBoxTable = scala.collection.mutable.Map[UUID, Set[ClientAddr]]()


  override def preStart() {
    info("started SilkMaster at %s", localhost)
  }

  protected def receive = {
    case RegisterClassBox(uuid, holder) =>
      val prevHolders : Set[ClientAddr] = classBoxTable.getOrElseUpdate(uuid, Set())
      classBoxTable += uuid -> (prevHolders + holder)
    case AskClassBoxHolder(uuid) =>
      if(classBoxTable.contains(uuid)) {
        val holder = classBoxTable(uuid)
        // TODO return a closest or free holder
        sender ! ClassBoxHolder(uuid, holder.head)
      }
      else {
        sender ! ClassBoxNotFound(uuid)
      }


  }

}