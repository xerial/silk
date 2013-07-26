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
import xerial.silk.SilkException
import xerial.silk.cluster.framework._

trait SilkService
  extends SilkFramework
  with SilkRunner
  with ZooKeeperService
  with DataProvider
  with LocalTaskManagerComponent
  with DistributedTaskMonitor
  with ClusterNodeManager
  with DistributedSliceStorage
  with DistributedCache
  with MasterRecordComponent
  with DefaultExecutor
  with DataServerComponent
  with ClassBoxComponentImpl
  with LocalClientComponent
  with SerializationService
  with Logger
{

  //type LocalClient = SilkClient
  def localClient = SilkClient.client.get

  val actorSystem : ActorSystem
  val localTaskManager = new LocalTaskManager {

    private def getMasterActorRef : Option[ActorRef] = {
      getMaster.map { m =>
        val masterAddr = s"${ActorService.AKKA_PROTOCOL}://silk@${m.address}:${m.port}/user/SilkMaster"
        actorSystem.actorFor(masterAddr)
      }
    }

    protected def sendToMaster(taskID: UUID, status: TaskStatus) {
      getMasterActorRef.map { master =>
        master ! TaskStatusUpdate(taskID, status)
      }
    }
    /**
     * Send the task to the master node
     * @param task
     */
    protected def sendToMaster(task: TaskRequest) {
      getMasterActorRef.map { master =>
        master ! task
      }
    }
  }

}

trait DataServerComponent {
  self: SilkFramework =>

  def dataServer : DataServer

}


trait ClassBoxComponentImpl extends ClassBoxComponent with IDUtil with SerializationService {
  self : SilkFramework with NodeManagerComponent with LocalClientComponent with CacheComponent =>

  type ClassBoxType = ClassBox

  private[this] val classBoxTable = collection.mutable.Map[UUID, ClassBox]()

  private def classBoxPath(cbid:UUID) = s"classbox/${cbid.prefix}"


  def classBoxID : UUID = {
    val cb = ClassBox.current
    classBoxTable.getOrElseUpdate(cb.id, {
      // register (nodeName, cb) pair to the cache
      cache.getOrElseUpdate(classBoxPath(cb.id), (localClient.currentNodeName, cb).serialize)
      cb
    })
    cb.id
  }

  /**
   * Retrieve the class box having the specified id
   * @param classBoxID
   * @return
   */
  def getClassBox(classBoxID:UUID) : ClassBox = {
    classBoxTable.getOrElseUpdate(classBoxID, {
      val path = classBoxPath(classBoxID)
      val pair : (String, ClassBox) = cache.getOrAwait(path).map(_.deserialize[(String, ClassBox)]).get
      val nodeName = pair._1
      val node = nodeManager.getNode(nodeName).getOrElse(SilkException.error(s"unknown node: $nodeName"))
      val cb = ClassBox.sync(pair._2, ClientAddr(node.host, node.clientPort))
      cb
    })
  }

}

