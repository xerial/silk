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


object TaskNode {
  implicit object TaskNodeOrdering extends Ordering[TaskNode] {
    def compare(x: TaskNode, y: TaskNode) = OrdPath.OrdPathOrdering.compare(x.id, y.id)
  }

}

case class TaskNode(id:OrdPath, op:Silk[_]) {

//  def split(numSplits:Int) :
}


object ScheduleGraph {

  /**
   * Create a DAGSchedule to build an evaluation schedule of the given Silk operation
   * @param op
   * @tparam A
   * @return
   */
  def apply[A](op:Silk[A]) : ScheduleGraph = {

    val dag = new ScheduleGraph()

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



class ScheduleGraph() {

  private var nodeCount = 0

  private val opSet = collection.mutable.Map.empty[Silk[_], TaskNode]
  private val outEdgeTable = collection.mutable.Map.empty[TaskNode, Set[TaskNode]]
  private val taskStatus = collection.mutable.Map.empty[OrdPath, TaskStatus]


  def nodes = opSet.values

  def inEdgesOf(node:TaskNode) =
    for((from, targetList) <- outEdgeTable if targetList.contains(node)) yield from

  def outEdgesOf(node:TaskNode) = outEdgeTable getOrElse(node, Seq.empty)


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
    val edgeList = for((src, lst) <- outEdgeTable; dest <- lst) yield src -> dest
    for((src, dest) <- edgeList.toSeq.sortBy(p => (p._2, p._1))) {
      s append s" ${src} -> ${dest}\n"
    }
    s.toString
  }

}



class DAGScheduler(dag:ScheduleGraph) {







}



trait StaticOptimizer {


  implicit class ToSeq(op:Silk[_])  {
    def asSeq = if(!op.isSingle) op.asInstanceOf[SilkSeq[_]] else SilkException.error(s"illegal conversion: ${op}")
  }

  def transform(op:Silk[_]) : Silk[_]

  def optimize(op:Silk[_]) : Silk[_] = {

    @tailrec
    def rTransform(a:Silk[_]) : Silk[_] = {
      val t = transform(a)
      if(t eq a) a else rTransform(t)
    }

    val opt = rTransform(op)
    opt match {
      case MapOp(id, fc, in, f) =>
        MapOp(id, fc, rTransform(in).asSeq, f)
      case FlatMapOp(id, fc, in, f) =>
        FlatMapOp(id, fc, rTransform(in).asSeq, f)
        // TODO add transformation for the other operations
      case JoinOp(id, fc, left, right, k1, k2) =>
        JoinOp(id, fc, rTransform(left).asSeq, rTransform(right).asSeq, k1, k2)
      case _ => opt
    }
  }

}

class DeforestationOptimizer extends StaticOptimizer {

  def transform(op:Silk[_]) : Silk[_] = {
    op match {
      case MapOp(id2, fc2, MapOp(id1, fc1, in, f1), f2) =>
        MapOp(id2, fc2, in, f1.andThen(f2))
      case _ => op
    }
  }
}

class MapReduceOptimizer extends StaticOptimizer {

  def transform(op: Silk[_]) = {
    op match {
      case SortOp(id, fc, in, ord, partitioner) =>
        val shuffler = ShuffleOp(SilkUtil.newUUID, fc, in, partitioner.asInstanceOf[Partitioner[Any]])
        val shuffleReducer = ShuffleReduceOp(id, fc, shuffler, ord.asInstanceOf[Ordering[Any]])
        shuffleReducer
      case _ => op
    }
  }
}





trait ScheduleOptimizerComponent {
  self: SilkFramework =>

  type StOptimizer <: StaticOptimizer

}






/**
 * @author Taro L. Saito
 */
trait TaskSchedulerComponent {
  self: SilkFramework =>


  def scheduler : TaskScheduler

  trait TaskScheduler {

    def eval[A](op:Silk[A]) {

      val dag = ScheduleGraph(op)

      // Make the updates of the schedule graph single-threaded as long as possible

      // Find unstarted nodes from the schedule graph

      // Submit the task to the master

      // Launch a monitor to mark










    }



  }

}