//--------------------------------------
//
// TaskStatus.scala
// Since: 2013/11/15 0:55
//
//--------------------------------------

package xerial.silk.framework.scheduler

import java.util.UUID
import xerial.silk.core.IDUtil


/**
 * Transaction record of task execution
 */
sealed trait TaskStatus

case object TaskAwaiting extends TaskStatus
case object TaskMissing extends TaskStatus
case object TaskReceived extends TaskStatus
case class TaskStarted(nodeName: String) extends TaskStatus

/**
 * Task finished record
 * @param nodeName
 */
case class TaskFinished(nodeName: String) extends TaskStatus
case class TaskFailed(nodeName: String, message: String) extends TaskStatus

case class TaskStatusUpdate(taskID: UUID, newStatus: TaskStatus) extends IDUtil {
  override def toString = s"[${taskID.prefix}] $newStatus"
}

