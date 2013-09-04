package xerial.silk.cluster.framework

import xerial.silk.framework._
import akka.actor.{ActorSystem, ActorContext, ActorRef, Actor}
import xerial.silk.framework.NodeRef
import xerial.silk.core.SilkSerializer

import xerial.silk.cluster.{SilkClient, ZooKeeperClient, config}
import org.apache.zookeeper.CreateMode
import xerial.core.log.Logger
import java.util.concurrent.TimeoutException
import xerial.silk.SilkException
import xerial.silk.cluster.SilkClient.Terminate

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
      synchronized {
        val remoteClient = me.context.system.actorFor(clientAddr)
        debug(s"Sending $request to ${nodeRef.name}")
        // TODO Retry
        remoteClient ! request
      }
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

object MasterRecord {

  def getMaster(zkc:ZooKeeperClient) : Option[MasterRecord] = {
    val mc = new MasterRecordComponent with ZooKeeperService {
      val zk = zkc
    }
    mc.getMaster
  }

}

/**
 * Recording master information to distributed cache
 */
trait MasterRecordComponent {
  self: ZooKeeperService =>

  import SilkSerializer._

  private implicit class Converter(b:Array[Byte]) {
    def toMasterRecord = b.deserializeAs[MasterRecord]
  }

  def getOrAwaitMaster : SilkFuture[MasterRecord] = {
    zk.getOrAwait(config.zk.masterInfoPath).map(_.deserializeAs[MasterRecord])
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


/**
 * Provides a function to create ActorRef
 */
trait SilkActorRefFactory {
  def actorRef(addr:String) : ActorRef
}


trait MasterFinder extends Logger {
  self: MasterRecordComponent with SilkActorRefFactory =>

  private var _master : ActorRef = null
  private var _currentMaster : Option[MasterRecord] = None

  def master: ActorRef = synchronized {
    import akka.pattern.ask
    import scala.concurrent.Await
    import scala.concurrent.duration._

    // Check the current master information
    val mr = getOrAwaitMaster.get
    if(!_currentMaster.exists(_ == mr)) {
      debug(s"The latest master: $mr")
      _currentMaster = Some(mr)

      // wait until the master is ready
      var timeout = 3.0
      val maxRetry = 10
      var retry = 0
      var masterIsReady = false
      var masterRef: ActorRef = null
      while (!masterIsReady && retry < maxRetry) {
        try {
          // Get an ActorRef of the SilkMaster
          val mr = getOrAwaitMaster.get
          val masterAddr = s"${ActorService.AKKA_PROTOCOL}://silk@${mr.address}:${mr.port}/user/SilkMaster"
          debug(s"Connecting to SilkMaster: $masterAddr, master host:${mr.name}")
          masterRef = actorRef(masterAddr)
          val ret = masterRef.ask(SilkClient.ReportStatus)(timeout.seconds)

          Await.result(ret, timeout.seconds)
          masterIsReady = true
          info(s"Connected to SilkMaster: $masterAddr")
        }
        catch {
          case e: TimeoutException =>
            warn(e)
            retry += 1
            timeout += timeout * 1.5
        }
      }

      if (!masterIsReady) {
        SilkException.error("Failed to find SilkMaster")
      }
      _master = masterRef
    }
    require(_master != null)
    _master
  }

}
