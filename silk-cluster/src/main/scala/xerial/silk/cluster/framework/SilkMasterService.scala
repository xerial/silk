package xerial.silk.cluster.framework

import xerial.silk.framework._
import akka.actor.Actor
import xerial.silk.framework.NodeRef


/**
 * @author Taro L. Saito
 */
trait SilkMasterService
  extends SilkFramework
  with ClusterResourceManager
  with ZooKeeperService
  with DefaultConsoleLogger
  with TaskManagerComponent
  with DistributedTaskMonitor
{
  me: Actor =>

  val taskManager = new TaskManagerImpl

  class TaskManagerImpl extends TaskManager {
    def dispatchTask(nodeRef: NodeRef, request: TaskRequest) {
      // Send the task to a remote client
      val clientAddr = s"${ActorService.AKKA_PROTOCOL}://silk@${nodeRef.address}:${nodeRef.clientPort}/user/SilkClient"
      val remoteClient = me.context.system.actorFor(clientAddr)
      remoteClient ! request
    }
  }


}
