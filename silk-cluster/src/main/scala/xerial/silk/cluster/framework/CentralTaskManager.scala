//--------------------------------------
//
// CentralTaskManager.scalar.scala
// Since: 2013/06/13 17:59
//
//--------------------------------------

package xerial.silk.cluster.framework

import xerial.silk.framework._
import java.util.UUID
import xerial.silk.mini.SilkMini
import com.netflix.curator.framework.api.CuratorWatcher
import scala.Some
import org.apache.zookeeper.WatchedEvent
import org.apache.zookeeper.Watcher.Event.{KeeperState, EventType}
import xerial.core.log.Logger
import scala.annotation.tailrec
import xerial.silk.{ConnectionLoss, SilkException}
import com.netflix.curator.framework.state.{ConnectionState, ConnectionStateListener}
import com.netflix.curator.framework.CuratorFramework


/**
 * @author Taro L. Saito
 */
trait CentralTaskManager
  extends TaskManagerComponent {
  self: ResourceManagerComponent with ZooKeeperService with TaskMonitorComponent =>

  val taskManager = new TaskManagerImpl

  class TaskManagerImpl extends TaskManager {
    def submitTask(nodeName: String, task: Task) {




    }
  }

}


/**
 * Distributed task monitor. Task states can be shared on all nodes
 */
trait DistributedTaskMonitor extends TaskMonitorComponent {
  self: ZooKeeperService =>

  val taskMonitor = new TaskMonitorImpl

  class TaskMonitorImpl extends TaskMonitor with Logger {

    import xerial.silk.cluster.config


    def statusPath(taskID:UUID) = {
      config.zk.clusterPath / "task" / taskID.prefix2 / taskID.prefix / "status"
    }

    def setStatus(taskID: UUID, status: TaskStatus) {
      zk.set(statusPath(taskID), status.serialize)
    }

    def getStatus(taskID: UUID) = {
      zk.get(statusPath(taskID)) match {
        case Some(b) => b.asTaskStatus
        case None => TaskMissing
      }
    }

    def completionFuture(taskID: UUID) : SilkFuture[TaskStatus] = {

      val f = new SilkFuture[TaskStatus] with CuratorWatcher with ConnectionStateListener with Guard { self =>
        // Monitor connection loss
        zk.curatorFramework.getConnectionStateListenable.addListener(self)

        val p = statusPath(taskID)
        val isUpdated = newCondition
        var currentStatus : TaskStatus = null
        var toContinue = true

        def isTerminated(ts:TaskStatus) = ts match {
          case TaskFinished(n) => true
          case TaskFailed(m) => true
          case TaskMissing => {
            warn(s"task:${taskID.prefix} is missing")
            false
          }
          case _ => false
        }

        def readStatus = {
          val ts = zk.getAndWatch(p, self) match {
            case Some(b) => b.asTaskStatus
            case None => TaskMissing
          }
          debug(s"read status: $ts")
          ts
        }

        def respond(k: (TaskStatus) => Unit) {
          guard {
            currentStatus = readStatus
            while(toContinue && !isTerminated(currentStatus)) {
              isUpdated.await()
            }
          }
          if(toContinue) {
            k(currentStatus)
          }
          else
            throw new ConnectionLoss()
        }

        private def signal = guard {
          isUpdated.signalAll()
        }

        def process(event: WatchedEvent) {

          event.getState match {
            case KeeperState.Disconnected =>
              toContinue = false
              signal
            case _ =>
          }

          event.getType match {
            case EventType.NodeCreated =>
              currentStatus = readStatus
              signal
            case EventType.NodeDataChanged =>
              currentStatus = readStatus
              signal
            case EventType.NodeDeleted =>
              currentStatus = TaskMissing
              signal
            case other =>
          }
        }

        def stateChanged(client: CuratorFramework, newState: ConnectionState) {
          newState match {
            case ConnectionState.SUSPENDED =>
              warn("connection to ZooKeeper is suspended")
              toContinue = false
              signal
            case ConnectionState.LOST =>
              warn("connection to ZooKeeper is lost")
              toContinue = false
              signal
            case _ =>
          }
        }
      }

      f
    }
  }


}