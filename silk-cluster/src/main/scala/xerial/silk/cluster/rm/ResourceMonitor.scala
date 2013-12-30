//--------------------------------------
//
// ResourceMonitor.scala
// Since: 2013/12/30 17:10
//
//--------------------------------------

package xerial.silk.cluster.rm

import akka.actor.{Props, Actor}
import scala.concurrent.duration._
import xerial.silk.cluster._
import xerial.silk.framework.{LifeCycle, NodeResourceState, LocalActorServiceComponent, LocalActorService}
import xerial.silk.sigar.SigarUtil
import xerial.core.log.Logger
import ZkPath._

object ResourceMonitor {

  case object Update

}
import ResourceMonitor._

trait ResourceMonitorComponent extends LifeCycle with Logger {

  self : SilkClusterFramework
    with ZooKeeperService
    with LocalClientComponent
    with LocalActorServiceComponent =>

  abstract override def startup {
    super.startup
    info("Start up ResourceMonitor")
    val rm = localActorService.actorOf(Props(new ResourceMonitorAgent(this)))
    import localActorService.dispatcher
    localActorService.scheduler.schedule(0.seconds, config.cluster.resourceMonitoringIntervalSec.seconds) {
      rm ! Update
    }
  }
  abstract override def teardown = {
    super.teardown
    info("Terminating ResourceMonitor")
  }

  val resourceMonitor : ResourceMonitor = new ResourceMonitor

  class ResourceMonitor {
    import xerial.silk.framework.SilkSerializer._

    def update = {
      val rs = NodeResourceState(SigarUtil.loadAverage, SigarUtil.freeMemory)
      zk.set(config.zk.clusterNodeStatusPath / localClient.currentNodeName, rs.serialize)
    }

    def get : NodeResourceState = get(localClient.currentNodeName)

    def get(nodeName:String) : NodeResourceState = {
      zk.get(config.zk.clusterNodeStatusPath / nodeName).map { b =>
        b.deserializeAs[NodeResourceState]
      } getOrElse NodeResourceState(Array(0.0, 0.0, 0.0), -1)
    }

  }

}


/**
 * Resource monitor
 *
 * @author Taro L. Saito
 */
class ResourceMonitorAgent(rc:ResourceMonitorComponent) extends Actor {
  def receive = {
    case Update =>
      rc.resourceMonitor.update
  }
}