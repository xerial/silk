//--------------------------------------
//
// SilkService.scala
// Since: 2013/06/24 1:38 PM
//
//--------------------------------------

package xerial.silk.cluster

import akka.actor.ActorSystem
import java.util.UUID
import xerial.silk._
import xerial.silk.cluster.store.{DataServerComponent, DistributedSliceStorage, DistributedCache}
import xerial.silk.framework._
import xerial.silk.cluster.rm.ClusterNodeManager
import xerial.silk.framework.scheduler.{TaskStatusUpdate, TaskStatus}
import xerial.silk.framework.scheduler.TaskStatusUpdate


trait ActorSystemComponent {
  val actorSystem : ActorSystem
  def actorRef(addr:String) = actorSystem.actorFor(addr)
}


/**
 * Any Silk programs must start up SilkService first to set up necessary components.
 * An instance of SilkService is available through Silk.env
 */
trait SilkService
  extends ClusterWeaver
  with ZooKeeperService
  with LocalTaskManagerComponent
  with DistributedTaskMonitor
  with ClusterNodeManager
  with DistributedSliceStorage
  with DistributedCache
  with DataServerComponent
  with ClassBoxComponent
  with LocalClientComponent
  with LocalClient
  with ActorSystemComponent
  with SerializationService
  with MasterRecordComponent
  with MasterFinder
  with SilkActorRefFactory
  with DefaultExecutor
{

  def localClient = this
  
  def currentNodeName = SilkCluster.localhost.prefix
  def address = SilkCluster.localhost.address

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

  def hosts = nodeManager.nodes

  override def weave[A](op:SilkSeq[A]) : SilkFuture[Seq[A]] = {
    executor.eval(op)
    new ConcreteSilkFuture(executor.run(SilkSession.defaultSession, op))
  }

  override def weave[A](op:SilkSingle[A]) : SilkFuture[A] = {
    executor.getSlices(op).head.map(slice => sliceStorage.retrieve(op.id, slice).head.asInstanceOf[A])
  }


  override private[silk] def runF0[R](locality:Seq[String], f: => R) = {
    localTaskManager.submit(classBox.classBoxID, locality)(f)
    null.asInstanceOf[R]
  }
}




