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

  self : ClusterWeaver
    with ZooKeeperService
    with LocalClientComponent
    with LocalActorServiceComponent =>

  abstract override def startup {
    super.startup
    info("Started ResourceMonitor")
    resourceMonitor.update
    val rm = localActorService.actorOf(Props(new ResourceMonitorAgent))
    import localActorService.dispatcher
    val interval = config.cluster.resourceMonitoringIntervalSec.seconds
    localActorService.scheduler.schedule(interval, interval) {
      rm ! Update
    }
  }

  abstract override def teardown = {
    super.teardown
    info("Terminating ResourceMonitor")
  }

  val resourceMonitor = new ResourceMonitor

  class ResourceMonitor extends Logger {
    import xerial.silk.framework.SilkSerializer._

    def update = {
      val nodeName = localClient.currentNodeName
      val rs = NodeResourceState(SigarUtil.loadAverage, SigarUtil.freeMemory)
      debug(s"[${nodeName}] Update resource info")
      zk.set(config.zk.clusterNodeStatusPath / nodeName, rs.serialize)
    }

    def get : NodeResourceState = get(localClient.currentNodeName)

    def get(nodeName:String) : NodeResourceState = {
      zk.get(config.zk.clusterNodeStatusPath / nodeName).map { b =>
        b.deserializeAs[NodeResourceState]
      } getOrElse NodeResourceState(Array(0.0, 0.0, 0.0), -1)
    }
  }

  class ResourceMonitorAgent extends Actor {
    def receive = {
      case Update =>
       resourceMonitor.update
    }
  }
}


