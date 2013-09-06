//--------------------------------------
//
// TaskScheduler.scala
// Since: 2013/09/03 9:21
//
//--------------------------------------

package xerial.silk.framework

import xerial.silk.Silk
import xerial.silk.index.OrdPath


object TaskNode {
  implicit object TaskNodeOrdering extends Ordering[TaskNode] {
    def compare(x: TaskNode, y: TaskNode) = OrdPath.OrdPathOrdering.compare(x.id, y.id)
  }

}

case class TaskNode(id:OrdPath, op:Silk[_])


object DAGSchedule {

  /**
   * Create a DAGSchedule to build an evaluation schedule of the given Silk operation
   * @param op
   * @tparam A
   * @return
   */
  def apply[A](op:Silk[A]) : DAGSchedule = {

    val dag = new DAGSchedule()

    def loop(s:Silk[_]) {
      dag.node(s) // Ensure the task node for the given Silk[_] exists
      for(in <- s.inputs) {
        dag.addEdge(in, s)
        loop(in)
      }
    }

    loop(op)
    dag
  }

}



class DAGSchedule() {

  private var nodeCount = 0

  private val opSet = collection.mutable.Map.empty[Silk[_], TaskNode]
  private val edgeTable = collection.mutable.Map.empty[TaskNode, Set[TaskNode]]
  private val taskStatus = collection.mutable.Map.empty[OrdPath, TaskStatus]

  private def node[A](op:Silk[A]) = {
    opSet.getOrElseUpdate(op, {
      nodeCount += 1
      val taskId = OrdPath(nodeCount)
      setStatus(taskId, TaskAwaiting)
      TaskNode(taskId, op)
    })
  }

  def addEdge[A, B](from:Silk[A], to:Silk[B]) {
    val a = node(from)
    val b = node(to)
    val outNodes = edgeTable.getOrElseUpdate(a, Set.empty)
    edgeTable += a -> (outNodes + b)
  }

  def setStatus(taskID:OrdPath, status:TaskStatus) {
    taskStatus += taskID -> status
  }

  override def toString = {
    val s = new StringBuilder
    s append "[nodes]\n"
    for (n <- opSet.values.toSeq.sorted)
      s append s" $n\n"

    s append "[edges]\n"
    val edgeList = for((src, lst) <- edgeTable; dest <- lst) yield src -> dest
    for((src, dest) <- edgeList.toSeq.sortBy(p => (p._2, p._1))) {
      s append s" ${src} -> ${dest}\n"
    }
    s.toString
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

      val dag = DAGSchedule(op)


    }



  }

}