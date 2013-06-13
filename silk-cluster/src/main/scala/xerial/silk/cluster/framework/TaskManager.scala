//--------------------------------------
//
// TaskManager.scala
// Since: 2013/06/13 17:59
//
//--------------------------------------

package xerial.silk.cluster.framework

import xerial.silk.framework.{ResourceManagerComponent, TaskMonitorComponent, TaskManagerComponent}
import java.util.UUID


/**
 * @author Taro L. Saito
 */
trait TaskManager
  extends TaskManagerComponent {
  self: ResourceManagerComponent with ZooKeeperService with TaskMonitorComponent =>


}

trait CentralTaskMonitor extends TaskMonitorComponent {
  self: ZooKeeperService =>

  val taskMonitor = new TaskMonitorImpl

  class TaskMonitorImpl extends TaskMonitor {


    def setStatus(task: Task, status: TaskStatus) {

    }
    def getStatus(taskID: UUID) = {

    }
    def waitCompletion(taskID: UUID) = {
      
    }
  }


}