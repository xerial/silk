//--------------------------------------
//
// SilkService.scala
// Since: 2013/06/24 1:38 PM
//
//--------------------------------------

package xerial.silk.cluster

import akka.actor.{ActorRef, ActorSystem}
import scala.language.experimental.macros
import java.util.UUID
import xerial.silk.framework._
import xerial.core.log.Logger
import xerial.silk.cluster.framework._

/**
 * Any Silk programs must start up SilkService first, to set up necessary components.
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
  with Logger
{

  //type LocalClient = SilkClient
  def localClient = this
  
  def currentNodeName = localhost.prefix
  def address = localhost.address

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



