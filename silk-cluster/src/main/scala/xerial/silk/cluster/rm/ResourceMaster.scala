//--------------------------------------
//
// ResourceMaster.scala
// Since: 2014/01/04 23:17
//
//--------------------------------------

package xerial.silk.cluster.rm

import akka.actor.Actor
import xerial.silk.cluster.{LocalInfoComponent, ClusterWeaver, ZooKeeperService}
import xerial.silk.framework.SharedStoreComponent


case class Resource(cpu:Double, memory:Long)

case class ResourceTable(table:Map[String, Resource])


trait ResourceMasterComponent extends ResourceTableAccessComponent {

  self: ClusterWeaver
    with LocalInfoComponent
    with SharedStoreComponent =>

  /**
   * @author Taro L. Saito
   */
  class ResourceMaster extends Actor {

    override def preStart() = {

    }


    def receive = {
      case _ =>
    }

  }

}