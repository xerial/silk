//--------------------------------------
//
// Silk.scala
// Since: 2013/06/24 1:38 PM
//
//--------------------------------------

package xerial.silk


import xerial.silk.cluster.{SilkClient, ZooKeeperClient, ZooKeeper}
import xerial.silk.cluster.framework._
import xerial.core.io.IOUtil
import akka.actor.{ActorRef, ActorSystem}
import scala.reflect.ClassTag
import xerial.silk.framework._
import xerial.silk.framework.ops._
import scala.language.experimental.macros
import xerial.silk.framework.ops.RawSeq
import java.util.UUID
import xerial.silk.framework.TaskRequest
import xerial.silk.framework.ops.RawSeq
import xerial.core.log.Logger

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
  with LocalClientComponent
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


/**
 * SilkEnv is an entry point of Silk functionality.
 */
class SilkEnv(@transient zk : ZooKeeperClient, @transient actorSystem : ActorSystem) extends SilkEnvLike { thisEnv =>

  @transient val service = new SilkService {
    val zk = thisEnv.zk
    val actorSystem = thisEnv.actorSystem
    def currentNodeName = xerial.silk.cluster.localhost.name
    def getLocalClient = SilkClient.client
  }

  def run[A](silk:Silk[A]) = {
    service.run(silk)
  }

  def newSilk[A](in:Seq[A])(implicit ev:ClassTag[A]) : SilkSeq[A] = macro SilkMacros.newSilkImpl[A]
  def scatter[A](in:Seq[A], numSplit:Int)(implicit ev:ClassTag[A]) : SilkSeq[A] = macro SilkMacros.newSilkSplitImpl[A]

  def sessionFor[A:ClassTag] = {
    import scala.reflect.runtime.{universe => ru}
    import ru._
    val t = scala.reflect.classTag[A]


  }

  def sendToRemote[A](seq: RawSeq[A], numSplit:Int = 1) {
    service.scatterData(seq, numSplit)
  }


}



/**
 * @author Taro L. Saito
 */
object SilkEnv {


  def silk[U](block: SilkEnv =>U):U = {
    import xerial.silk.cluster._
    val result = for{
      zk <- ZooKeeper.defaultZkClient
      actorSystem <- ActorService(localhost.address, IOUtil.randomPort)
    } yield {
      val env = new SilkEnv(zk, actorSystem)
      block(env)
    }
    result.head
  }
}