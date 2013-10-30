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



object TaskScheduler {

  def defaultStaticOptimizers = Seq(new DeforestationOptimizer, new ShuffleReduceOptimizer)

  case object Start
  case object Timeout
  case class EnqueueTask(task:TaskNode)
}


case class TaskUpdate(id:OrdPath, newStatus:TaskStatus)

/**
 * TaskScheduler sends task requests to the master then monitors task status
 * update notifications, update the schedule graph and submit next eligible tasks.
 *
 * @tparam A
 */
class TaskScheduler[A](sg:ScheduleGraph)
  extends Actor with Logger {

  private lazy val taskQueue = context.system.actorFor(s"${context.parent.path}/taskQueue")

  import TaskScheduler._

  def receive = {
    case Start =>
      evalNext
    case u@TaskUpdate(id, newStatus) =>
      debug(s"Received $u")
      sg.setStatus(id, newStatus)
      newStatus match {
        case TaskReceived =>
        case TaskStarted(node) =>
        case TaskFinished(node) =>
          evalNext
        case TaskFailed(node, e) =>
          warn(newStatus)
        case _ =>
      }
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
      taskQueue ! EnqueueTask(task)
    }
  }

  override def preStart() {
    debug("started")
  }

  override def postStop() {
    debug("terminated")
  }
}




class TaskQueue extends Actor with Logger {

  import TaskScheduler._

  def receive = {
    case EnqueueTask(t) =>
      debug(s"Received a task:$t")
      sender ! TaskUpdate(t.id, TaskReceived)
      eval(t)
  }

  def eval(task:TaskNode) {
    val node = Silk.localhost.name

    sender ! TaskUpdate(task.id, TaskStarted(node))
    try {
      evalOp(task.op)
//      sender ! TaskUpdate(task.id, TaskFinished(node))
    }
    catch {
      case e:Exception =>
        sender ! TaskUpdate(task.id, TaskFailed(node, e.getMessage))
    }
  }

  def evalOp(op:Silk[_]) {
    op match {
      case RawSeq(id, fc, seq) =>
        // Register seq to data server


      case _ => SilkException.error(s"unknown op: ${op}")
    }
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

      // Static optimization
      debug(s"Apply static optimization to ${op}")
      val staticOptimizers = TaskScheduler.defaultStaticOptimizers
      val optimized = staticOptimizers.foldLeft[Silk[_]](op){(op, optimizer) => optimizer.optimize(op)}

      // Create a schedule graph
      val sg = ScheduleGraph(optimized)
      debug(s"Schedule graph:\n$sg")

      // Launch TaskScheduler and submitter
      for(as <- ActorService.local) {
        val taskQueue = as.actorOf(Props[TaskQueue], name="taskQueue")
        val schedulerRef = as.actorOf(Props(new TaskScheduler(sg)), name="scheduler")

        // Tick scheduler periodically
        import scala.concurrent.duration._
        import as.dispatcher
        as.scheduler.scheduleOnce(timeout.seconds){ schedulerRef ! TaskScheduler.Timeout }

        // Start evaluation
        schedulerRef ! TaskScheduler.Start

        // Await termination of the scheduler
        as.awaitTermination()
      }
    }
  }

}





