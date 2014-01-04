//--------------------------------------
//
// ResourceMaster.scala
// Since: 2014/01/04 23:17
//
//--------------------------------------

package xerial.silk.cluster.rm

import akka.actor.{Props, Actor}
import xerial.silk.cluster.{LocalInfoComponent, ClusterWeaver, ZooKeeperService}
import xerial.silk.framework._
import java.util.UUID
import xerial.silk.framework.NodeState


object ResourceMaster {

  sealed trait Reply

  case class Granted(uuid:UUID) extends Reply
  case class PartiallyGranted(granted:ClusterResource) extends Reply
  case class Denied(uuid:UUID) extends Reply

  object Priority {
    val HIGH = 10
    val DEFAULT = 5
    val LOW = 0
  }
}

/**
 * Resource master table that resides in SilkMaster
 */
trait ResourceMasterComponent {


  self: ClusterWeaver
    with SharedStoreComponent
    with ResourceStateAccessComponent
  =>

  val resourceMaster : ResourceMaster = new ResourceMaster

  class ResourceMaster {

    import ResourceMaster._

    private val table = collection.mutable.Map[String, NodeState]()

    def init {
      for((node, res) <- resourceTable.all) yield {
        table += node -> res
      }
      debug(table.mkString(", "))
    }

    def ask(clusterResource:ClusterResource) : Reply = {
      debug(s"Resource request: $clusterResource")

      // TODO resource allocation transaction


      Denied(clusterResource.uuid)
    }


  }


}