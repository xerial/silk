//--------------------------------------
//
// TaskScheduler.scala
// Since: 2013/09/03 9:21
//
//--------------------------------------

package xerial.silk.framework

import java.util.UUID
import xerial.silk.Silk
import xerial.silk.framework.ops.CallGraph


case class TaskNode(id:Int, op:Silk[_])


object DAGSchedule {

  def apply[A](op:Silk[A]) : DAGSchedule = {
    val g = CallGraph(op)

    // Silk[A] -> DAGSchedule ->


    null
  }

}

class DAGSchedule() {
  private var nodes = Set.empty[TaskNode]
  private var edges = Map.empty[TaskNode, Seq[TaskNode]]
  private var taskStatus = Map.empty[Int, TaskStatus]

  def root = _

  def setStatus(taskID:Int, status:TaskStatus) {
    taskStatus += taskID -> status
  }





}

/**
 * @author Taro L. Saito
 */
trait TaskSchedulerComponent {
  self: SilkFramework =>


  def scheduler : TaskScheduler

  trait TaskScheduler {

    def eval[A](op:Silk[A]) {



    }



  }

}