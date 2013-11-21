//--------------------------------------
//
// CentralTaskManager.scalar.scala
// Since: 2013/06/13 17:59
//
//--------------------------------------

package xerial.silk.cluster

import java.util.UUID
import com.netflix.curator.framework.api.CuratorWatcher
import org.apache.zookeeper.WatchedEvent
import org.apache.zookeeper.Watcher.Event.{KeeperState, EventType}
import xerial.core.log.Logger
import java.util.concurrent.TimeUnit
import xerial.silk.util.Guard
import xerial.silk.framework._
import xerial.silk.framework.scheduler.{TaskFinished, TaskFailed, TaskMissing, TaskStatus}
import xerial.silk.SilkFuture


/**
 * Distributed task monitor. Task states can be shared on all nodes
 */
trait DistributedTaskMonitor extends TaskMonitorComponent {
  self: SilkClusterFramework with ZooKeeperService =>

  val taskMonitor = new TaskMonitorImpl

  class TaskMonitorImpl extends TaskMonitor with Logger {



    def statusPath(taskID:UUID) = {
      config.zk.clusterPath / "task" / taskID.prefix2 / taskID.prefix / "status"
    }

    def setStatus(taskID: UUID, status: TaskStatus) {
      trace(s"Set task status ${taskID.prefix}: $status")
      zk.set(statusPath(taskID), status.serialize)
    }

    def getStatus(taskID: UUID) = {
      zk.get(statusPath(taskID)) match {
        case Some(b) => b.asTaskStatus
        case None => TaskMissing
      }
    }

    def completionFuture(taskID: UUID) : SilkFuture[TaskStatus] = new CompletionFuture(taskID)

    class CompletionFuture(taskID:UUID)
      extends SilkFuture[TaskStatus]
      with CuratorWatcher
      with Guard { self =>

      private val p = statusPath(taskID)
      private val isUpdated = newCondition
      @volatile private var currentStatus : TaskStatus = null
      @volatile private var toContinue = true

      private def isTerminated = currentStatus match {
        case TaskFinished(nodeName) =>
          true
        case TaskFailed(nodeName, m) => true
        case TaskMissing => {
          trace(s"task:${taskID.prefix} is missing")
          false
        }
        case other =>
          false
      }

      private def readStatus {
        currentStatus = zk.get(p) match {
          case Some(b) => b.asTaskStatus
          case None => TaskMissing
        }
        if(!isTerminated)
          zk.curatorFramework.checkExists().usingWatcher(self).forPath(p.path)
        trace(s"read status: $currentStatus, ${taskID.prefix}")
      }

      def respond(k: TaskStatus => Unit) : Unit =  {
        guard {
          while(toContinue && !isTerminated) {
            readStatus
            if(!isTerminated)
              isUpdated.await(30, TimeUnit.SECONDS)
          }
        }
        k(currentStatus)
      }

      private def signal : Unit = guard {
        isUpdated.signalAll()
      }

      def process(event: WatchedEvent) {
        event.getState match {
          case KeeperState.Disconnected =>
            toContinue = false
          case KeeperState.Expired =>
            toContinue = false
          case _ =>
        }

        event.getType match {
          case EventType.NodeCreated =>
          case EventType.NodeDataChanged =>
          case EventType.NodeDeleted =>
            currentStatus = TaskMissing
          case other =>
        }
        guard {
          isUpdated.signalAll()
        }
      }
    }
  }

}