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
import xerial.core.log.Logger

import akka.actor.{ActorRef, ActorSystem, Props, Actor}


object TaskNode {

  implicit object TaskNodeOrdering extends Ordering[TaskNode] {
    def compare(x:TaskNode, y:TaskNode) = OrdPath.OrdPathOrdering.compare(x.id, y.id)
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

  import scala.collection.mutable

  private val opSet = mutable.Map.empty[Silk[_], TaskNode]
  private val outEdgeTable = mutable.Map.empty[TaskNode, Set[TaskNode]]
  private val taskStatus = mutable.Map.empty[OrdPath, TaskStatus]


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
    val edgeList = for ((src, lst) <- outEdgeTable; dest <- lst) yield src -> dest
    for ((src, dest) <- edgeList.toSeq.sortBy(p => (p._2, p._1))) {
      s append s" ${src.id} -> ${dest.id}\n"
    }
    s.toString
  }

}




object TaskScheduler {

  def defaultStaticOptimizers = Seq(new DeforestationOptimizer, new ShuffleReduceOptimizer)

  case object Start
  case object Timeout
  case class DispatchTask(task:TaskNode)
}


case class TaskUpdate(id:OrdPath, newStatus:TaskStatus)

class TaskScheduler[A](op:Silk[A], submitter:ActorRef,
                       staticOptimizers:Seq[StaticOptimizer] = TaskScheduler.defaultStaticOptimizers)
  extends Actor with Logger {

  import TaskScheduler._

  // Apply static code optimization
  private val optimized = {
    debug(s"Apply static optimization to ${op}")
    staticOptimizers.foldLeft[Silk[_]](op){(op, optimizer) => optimizer.optimize(op)}
  }

  // Create a schedule graph
  private val sg = {
    val g = ScheduleGraph(optimized)
    debug(s"Schedule graph:\n$g")
    g
  }

  def receive = {
    case Start =>
      evalNext
    case u@TaskUpdate(id, newStatus) =>
      debug(u)
      sg.setStatus(id, newStatus)
      evalNext
    case Timeout =>
      warn(s"Timed out: shut down the scheduler")
      context.system.shutdown()
  }


  private def evalNext {
    // Find unstarted and eligible tasks from the schedule graph
    val eligibleTasks = sg.eligibleNodesForStart
    for(task <- eligibleTasks) {
      debug(s"Evaluate $task")
      // TODO: Dynamic optimization according to the available cluster resources
      // Submit the task
      submitter ! DispatchTask(task)
    }
  }


  override def preStart() {
    debug("started")
  }

  override def postStop() {
    debug("terminated")
  }
}

class TaskRunner extends Actor {
  def receive = ???
}



class TaskSubmitter extends Actor with Logger {

  import TaskScheduler._

  def receive = {
    case DispatchTask(t) =>
      debug(s"Received a task:$t")
      sender ! TaskUpdate(t.id, TaskReceived)

  }
}


/**
 * @author Taro L. Saito
 */
trait TaskSchedulerComponent  {
  self:SilkFramework =>


  def scheduler:TaskSchedulerAPI

  trait TaskSchedulerAPI extends Logger {

    val timeout = 60

    def eval[A](op:Silk[A]) {
      for(as <- ActorService.local) {
        val submitterRef = as.actorOf(Props[TaskSubmitter], name="submitter")
        val schedulerRef = as.actorOf(Props(new TaskScheduler(op, submitterRef)), name="scheduler")


        // Tick scheduler periodically
        import scala.concurrent.duration._
        import as.dispatcher
        as.scheduler.scheduleOnce(timeout.seconds){ schedulerRef ! TaskScheduler.Timeout }
        // Start evaluation
        schedulerRef ! TaskScheduler.Start

        as.awaitTermination()
      }
    }
  }

}





