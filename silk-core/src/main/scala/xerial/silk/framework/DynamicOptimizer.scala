//--------------------------------------
//
// DynamicOptimizer.scala
// Since: 2013/10/22 4:32 PM
//
//--------------------------------------

package xerial.silk.framework

import xerial.silk.framework.scheduler.TaskNode

trait RuntimeResource {

  def numNodes : Int


}


/**
 * Optimize task schedule according to the available machine resource
 *
 *
 * @author Taro L. Saito
 */
trait DynamicOptimizer {
  def optimize(task:TaskNode, rt:RuntimeResource) : TaskNode

}

class TaskSplitOptimizer extends DynamicOptimizer {
  def optimize(task:TaskNode, rt:RuntimeResource) : TaskNode = {

    task.op match {
      case _ => task
    }
  }

}