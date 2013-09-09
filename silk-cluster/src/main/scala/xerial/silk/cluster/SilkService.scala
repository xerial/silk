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
    classBoxTable.getOrElseUpdate(cbLocal.id, {
      val cb = ClassBox(localhost.address, dataServer.port, cbLocal.entries)
      // register (nodeName, cb) pair to the cache
      cache.update(classBoxPath(cb.id), cb.serialize)
      dataServer.register(cb)
      cb
    })
    cbLocal.id
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

