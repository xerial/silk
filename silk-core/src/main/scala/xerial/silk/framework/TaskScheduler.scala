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


case class TaskNode(id:Int, op:Silk[_], status:TaskStatus, parentTask:Option[TaskNode], dependentTask:Seq[TaskNode])


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