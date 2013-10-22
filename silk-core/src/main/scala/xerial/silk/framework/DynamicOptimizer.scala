//--------------------------------------
//
// DynamicOptimizer.scala
// Since: 2013/10/22 4:32 PM
//
//--------------------------------------

package xerial.silk.framework

/**
 * Optimize task schedule according to the available machine resource
 *
 *
 * @author Taro L. Saito
 */
trait DynamicOptimizer {
  def optimize(task:TaskNode) : TaskNode

}

class TaskSplitOptimizer extends DynamicOptimizer {
  def optimize(task:TaskNode) : TaskNode = {

    task.op match {
      case _ => task
    }
  }

}