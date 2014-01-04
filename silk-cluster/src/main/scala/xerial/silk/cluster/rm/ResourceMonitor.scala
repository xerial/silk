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
import xerial.silk.framework._
import xerial.silk.sigar.SigarUtil
import xerial.core.log.Logger
import ZkPath._
import xerial.silk.framework.NodeResourceState

object ResourceMonitor {

  case object Update

}
import ResourceMonitor._

/**
 * Service that periodically monitors available CPU and memory resources
 */
trait ResourceMonitorComponent
  extends LifeCycle
  with ResourceTableAccessComponent
  with Logger {

  self : ClusterWeaver
    with SharedStoreComponent
    with LocalInfoComponent
    with LocalActorServiceComponent =>

  abstract override def startup {
    super.startup
    info("Started ResourceMonitor")
    resourceTable.update
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

  class ResourceMonitorAgent extends Actor {
    def receive = {
      case Update =>
       resourceTable.update
    }
  }
}



/**
 * Provides access to resource table
 */
trait ResourceTableAccessComponent extends Logger {
  self: ClusterWeaver
    with SharedStoreComponent
    with LocalInfoComponent =>

  import xerial.silk.framework.SilkSerializer._

  val resourceTable = new ResourceTableAccess

  class ResourceTableAccess extends Logger {
    def get : NodeResourceState = get(currentNodeName)
    def get(nodeName:String) : NodeResourceState = {
      store.get((config.zk.clusterNodeStatusPath / nodeName).path).map { b =>
        b.deserializeAs[NodeResourceState]
      } getOrElse NodeResourceState(Array(0.0, 0.0, 0.0), -1)
    }

    def update = {
      val rs = NodeResourceState(SigarUtil.loadAverage, SigarUtil.freeMemory)
      trace(s"[${currentNodeName}] Update resource info: $rs")
      store((config.zk.clusterNodeStatusPath / currentNodeName).path) = rs.serialize
    }
  }

}

