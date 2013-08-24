package xerial.silk.cluster.framework

import xerial.silk.framework._
import akka.actor.Actor
import xerial.silk.framework.NodeRef
import xerial.silk.core.SilkSerializer

import xerial.silk.cluster.config
import org.apache.zookeeper.CreateMode

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
  with DistributedCache
  with MasterRecordComponent
{
  me: Actor =>

  val name:String
  val address:String

  val taskManager = new TaskManagerImpl

  class TaskManagerImpl extends TaskManager {
    def dispatchTask(nodeRef: NodeRef, request: TaskRequest) {
      // Send the task to a remote client
      val clientAddr = s"${ActorService.AKKA_PROTOCOL}://silk@${nodeRef.address}:${nodeRef.clientPort}/user/SilkClient"
      val remoteClient = me.context.system.actorFor(clientAddr)

      debug(s"Sending $request to ${nodeRef.name}")

      // TODO Retry
      remoteClient ! request
    }
  }

  abstract override def startup {
    super.startup
    setMaster(name, address, config.silkMasterPort)
  }
  abstract override def teardown {
    super.teardown
  }
}


case class MasterRecord(name:String, address:String, port:Int)

/**
 * Recording master information to distributed cache
 */
trait MasterRecordComponent {
  self: ZooKeeperService with CacheComponent =>

  import SilkSerializer._

  private implicit class Converter(b:Array[Byte]) {
    def toMasterRecord = b.deserializeAs[MasterRecord]
  }

  def getOrAwaitMaster : SilkFuture[MasterRecord] = {
    cache.getOrAwait(config.zk.masterInfoPath.path).map(_.deserializeAs[MasterRecord])
  }

  def getMaster : Option[MasterRecord] = {
    val p = config.zk.masterInfoPath
    if(zk.exists(p))
      Some(zk.read(p).toMasterRecord)
    else
      None
  }

  def setMaster(name:String, address:String, port:Int) = {
    val p = config.zk.masterInfoPath
    zk.set(p, MasterRecord(name, address, port).serialize, CreateMode.EPHEMERAL)
  }

}
