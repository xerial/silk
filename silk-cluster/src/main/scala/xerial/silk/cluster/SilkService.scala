//--------------------------------------
//
// SilkService.scala
// Since: 2013/06/24 1:38 PM
//
//--------------------------------------

package xerial.silk.cluster

import xerial.core.io.IOUtil
import akka.actor.{ActorRef, ActorSystem}
import scala.reflect.ClassTag
import scala.language.experimental.macros
import java.util.UUID
import xerial.silk.framework._
import xerial.core.log.Logger
import xerial.silk.{SilkException, SilkEnv, Silk}
import xerial.silk.cluster.framework._
import xerial.silk.framework.ops.RawSeq

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


/**
 * @author Taro L. Saito
 */
object SilkEnvImpl {

  def silk[U](block: => U):U = {
    val result = for{
      zk <- ZooKeeper.defaultZkClient
      actorSystem <- ActorService(localhost.address, IOUtil.randomPort)
    } yield {
      val env = new SilkEnvImpl(zk, actorSystem)
      Silk.setEnv(env)
      block
    }
    result.head
  }
}
/**
 * SilkEnv is an entry point of Silk functionality.
 */
class SilkEnvImpl(@transient zk : ZooKeeperClient, @transient actorSystem : ActorSystem) extends SilkEnv { thisEnv =>

  @transient val service = new SilkService {
    val zk = thisEnv.zk
    val actorSystem = thisEnv.actorSystem
    def currentNodeName = xerial.silk.cluster.localhost.name
    def getLocalClient = SilkClient.client
  }

  def run[A](silk:Silk[A]) = {
    service.run(silk)
  }
  def run[A](silk: Silk[A], target: String) = {
    service.run(silk, target)
  }

  def sessionFor[A:ClassTag] = {
    import scala.reflect.runtime.{universe => ru}
    import ru._
    val t = scala.reflect.classTag[A]
  }

  def sendToRemote[A](seq: RawSeq[A], numSplit:Int = 1) = {
    service.scatterData(seq, numSplit)
    seq
  }


  private[silk] def runF0[R](locality:Seq[String], f: => R) = {
    val task = service.localTaskManager.submit(service.classBoxID, locality)(f)
    // TODO retrieve result
    null.asInstanceOf[R]
  }
}



