//--------------------------------------
//
// ResourceMonitor.scala
// Since: 2013/12/30 17:10
//
//--------------------------------------

package xerial.silk.cluster.rm

import akka.actor.{Props, Actor}
import xerial.silk.framework.scheduler.TaskScheduler
import scala.concurrent.duration._
import xerial.silk.cluster._
import xerial.silk.framework.{NodeResourceState, LocalActorServiceComponent, LocalActorService}
import xerial.silk.sigar.Sigar

object ResourceMonitor {


    //as.scheduler.scheduleOnce(taskDispatcherTimeout.seconds){ schedulerRef ! TaskScheduler.Timeout }

  case object Update

}
import ResourceMonitor._

trait ResourceMonitorComponent {

  self : SilkClusterFramework
    with ZooKeeperService
    with LocalClientComponent
    with LocalActorServiceComponent =>

  {
    val rm = localActorService.actorOf(Props(new ResourceMonitor(localClient.currentNodeName, config.zk.clusterNodePath ,zk)))
    import localActorService.dispatcher
    localActorService.scheduler.schedule(0.seconds, config.cluster.resourceMonitoringIntervalSec.seconds) {
      rm ! Update
    }
  }

}


/**
 * Resource monitor
 *
 * @author Taro L. Saito
 */
class ResourceMonitor(nodeName:String, nodePath:ZkPath, zk:ZooKeeperClient) extends Actor {

  import ZkPath._
  import xerial.silk.framework.SilkSerializer._

  def receive = {
    case Update =>
       val sigar = new Sigar
       val rs = NodeResourceState(sigar.loadAverage().toDouble, sigar.free().toLong)
       zk.set(nodePath / nodeName, rs.serialize)
  }
}