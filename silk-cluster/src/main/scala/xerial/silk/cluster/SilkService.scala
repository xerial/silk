//--------------------------------------
//
// SilkService.scala
// Since: 2013/06/24 1:38 PM
//
//--------------------------------------

package xerial.silk.cluster

import akka.actor.{ActorSelection, ActorRef, ActorSystem}
import scala.language.experimental.macros
import java.util.UUID
import xerial.silk.framework._
import xerial.core.log.Logger
import xerial.silk.SilkException
import xerial.silk.cluster.framework._

/**
 * Any Silk programs must start up SilkService first, to set up necessary components.
 * SilkService is available through Silk.env
 */
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
  with LocalClientAPI
  with Logger
{

  //type LocalClient = SilkClient
  def localClient = SilkClient.client.get

  def currentNodeName = localhost.prefix
  def address = localhost.address

  val actorSystem : ActorSystem
  val localTaskManager = new LocalTaskManager {

    private def getMasterActorRef : Option[ActorSelection] = {
      getMaster.map { m =>
        val masterAddr = s"${ActorService.AKKA_PROTOCOL}://silk@${m.address}:${m.port}/user/SilkMaster"
        actorSystem.actorSelection(masterAddr)
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
  self : SilkFramework
    with CacheComponent
    with DataServerComponent
  =>

  type ClassBoxType = ClassBox

  private[this] val classBoxTable = collection.mutable.Map[UUID, ClassBox]()

  private def classBoxPath(cbid:UUID) = s"classbox/${cbid.prefix}"


  def classBoxID : UUID = synchronized {
    val cbLocal = ClassBox.current
    val cb = ClassBox(localhost.address, dataServer.port, cbLocal.entries)
    classBoxTable.getOrElseUpdate(cb.id, {
      // register (nodeName, cb) pair to the cache
      cache.update(classBoxPath(cb.id), cb.serialize)
      dataServer.register(cb)
      cb
    })
    cb.id
  }

  /**
   * Retrieve the class box having the specified id
   * @param classBoxID
   * @return
   */
  def getClassBox(classBoxID:UUID) : ClassBox = synchronized {
    classBoxTable.getOrElseUpdate(classBoxID, {
      val path = classBoxPath(classBoxID)
      val remoteCb : ClassBox= cache.getOrAwait(path).map(_.deserialize[ClassBox]).get
      val cb = ClassBox.sync(remoteCb)
      // Register retrieved class box
      cache.getOrElseUpdate(classBoxPath(cb.id), cb.serialize)
      cb
    })
  }

}

