//--------------------------------------
//
// TaskScheduler.scala
// Since: 2013/09/03 9:21
//
//--------------------------------------

package xerial.silk.framework

import xerial.silk._
import xerial.silk.index.OrdPath
import xerial.silk.framework.ops._
import java.util.UUID
import scala.annotation.unchecked.uncheckedVariance
import xerial.silk.framework.ops.ShuffleReduceOp
import xerial.silk.framework.ops.ShuffleOp
import xerial.silk.framework.ops.SortOp
import xerial.silk.framework.ops.MapOp
import scala.annotation.tailrec
import xerial.core.log.Logger


object TaskNode {

  implicit object TaskNodeOrdering extends Ordering[TaskNode] {
    def compare(x:TaskNode, y:TaskNode) = OrdPath.OrdPathOrdering.compare(x.id, y.id)
  }

}

case class TaskNode(id:OrdPath, op:Silk[_], subTasks:IndexedSeq[TaskNode]) {

  //  def split(numSplits:Int) :
}


object ScheduleGraph {

  /**
   * Create a DAGSchedule to build an evaluation schedule of the given Silk operation
   * @param op
   * @tparam A
   * @return
   */
  def apply[A](op:Silk[A]):ScheduleGraph = {

    val dag = new ScheduleGraph()

    def loop(s:Silk[_]) {
      dag.node(s) // Ensure the task node for the given Silk[_] exists
      for (in <- s.inputs) {
        dag.addEdge(in, s)
        loop(in)
      }
    }

    loop(op)
    dag
  }

}


class ScheduleGraph() {

  private var nodeCount = 0

  private val opSet = collection.mutable.Map.empty[Silk[_], TaskNode]
  private val outEdgeTable = collection.mutable.Map.empty[TaskNode, Set[TaskNode]]
  private val taskStatus = collection.mutable.Map.empty[OrdPath, TaskStatus]


  def nodes = opSet.values

  def inEdgesOf(node:TaskNode) =
    for ((from, targetList) <- outEdgeTable if targetList.contains(node)) yield from

  def outEdgesOf(node:TaskNode) = outEdgeTable getOrElse(node, Seq.empty)

  def status(node:TaskNode) = {
    taskStatus.get(node.id).getOrElse(TaskAwaiting)
  }


  def eligibleNodesForStart = {

    def isAwaiting(t:TaskNode) = status(t) == TaskAwaiting
    def isFinished(t:TaskNode) = status(t) match { case TaskFinished(_) => true; case _ => false }

    for(n <- nodes if isAwaiting(n) && inEdgesOf(n).forall(isFinished)) yield n
  }


  private def node[A](op:Silk[A]) = {
    opSet.getOrElseUpdate(op, {
      nodeCount += 1
      val taskId = OrdPath(nodeCount)
      setStatus(taskId, TaskAwaiting)
      TaskNode(taskId, op, IndexedSeq.empty)
    })
  }

  def addEdge[A, B](from:Silk[A], to:Silk[B]) {
    val a = node(from)
    val b = node(to)
    val outNodes = outEdgeTable.getOrElseUpdate(a, Set.empty)
    outEdgeTable += a -> (outNodes + b)
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
    val edgeList = for ((src, lst) <- outEdgeTable; dest <- lst) yield src -> dest
    for ((src, dest) <- edgeList.toSeq.sortBy(p => (p._2, p._1))) {
      s append s" ${src} -> ${dest}\n"
    }
    s.toString
  }

}


class DAGScheduler(dag:ScheduleGraph) {


}



/**
 * @author Taro L. Saito
 */
trait TaskSchedulerComponent {
  self:SilkFramework =>


  def scheduler:TaskScheduler

  trait TaskScheduler extends Logger {

    def eval[A](op:Silk[A]) {

      // Apply static code optimization
      val staticOptimizers = Seq(new DeforestationOptimizer, new ShuffleReduceOptimizer)
      val optimized = staticOptimizers.foldLeft[Silk[_]](op){(op, optimizer) => optimizer.optimize(op)}

      // Create a schedule graph
      val sg = ScheduleGraph(optimized)
      debug(s"Schedule graph:\n$sg")

      // Find unstarted and eligible nodes from the schedule graph
      val eligibleNodes = sg.eligibleNodesForStart
      debug(s"eligible nodes: ${eligibleNodes.mkString(", ")}")

      // Dynamic optimization according to the available cluster resources




      // Make the updates of the schedule graph single-threaded as long as possible


      // Submit the task to the master

      // Launch a monitor to mark


    }


  }

}