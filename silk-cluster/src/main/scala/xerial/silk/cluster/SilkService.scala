//--------------------------------------
//
// SilkService.scala
// Since: 2013/06/24 1:38 PM
//
//--------------------------------------

package xerial.silk.cluster

import akka.actor.ActorSystem
import java.util.UUID
import xerial.silk.{SilkException, Silk}
import xerial.silk.cluster.store.{DataServerComponent, DistributedSliceStorage, DistributedCache}
import xerial.silk.framework._
import xerial.silk.cluster.rm.ClusterNodeManager
import xerial.silk.framework.scheduler.{TaskStatusUpdate, TaskStatus}


/**
 * Any Silk programs must start up SilkService first to set up necessary components.
 * An instance of SilkService is available through Silk.env
 */
trait SilkService
  extends SilkFramework
  with SilkRunner
  with ZooKeeperService
  with LocalTaskManagerComponent
  with DistributedTaskMonitor
  with ClusterNodeManager
  with DistributedSliceStorage
  with DistributedCache
  with DefaultExecutor
  with DataServerComponent
  with ClassBoxComponentImpl
  with LocalClientComponent
  with LocalClient
  with SerializationService
  with MasterRecordComponent
  with MasterFinder
  with SilkActorRefFactory
{

  //type LocalClient = SilkClient
  def localClient = this
  
  def currentNodeName = SilkCluster.localhost.prefix
  def address = SilkCluster.localhost.address

  val actorSystem : ActorSystem
  def actorRef(addr:String) = actorSystem.actorFor(addr)

  val localTaskManager = new LocalTaskManager {
    protected def sendToMaster(taskID: UUID, status: TaskStatus) {
      synchronized {
        master ! TaskStatusUpdate(taskID, status)
      }
    }
    protected def sendToMaster(task: TaskRequest) {
      synchronized {
        master ! task
      }
    }
  }

}




trait SilkRunner extends SilkFramework with ProgramTreeComponent {
  self: ExecutorComponent =>

  def eval[A](silk:Silk[A]) = executor.eval(silk)

  /**
   * Evaluate the silk using the default session
   * @param silk
   * @tparam A
   * @return
   */
  def run[A](silk:Silk[A]) : Result[A] = run(SilkSession.defaultSession, silk)
  def run[A](silk:Silk[A], target:String) : Result[_] = {
    ProgramTree.findTarget(silk, target).map { t =>
      run(t)
    } getOrElse { SilkException.error(s"target $target is not found") }
  }

  def run[A](session:Session, silk:Silk[A]) : Result[A] = {
    executor.run(session, silk)
  }

}
