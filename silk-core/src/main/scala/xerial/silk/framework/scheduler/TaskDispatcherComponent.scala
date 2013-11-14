//--------------------------------------
//
// TaskDispatcherComponent.scala
// Since: 2013/11/12 10:49
//
//--------------------------------------

package xerial.silk.framework.scheduler

import xerial.silk.Silk
import xerial.core.log.LoggerFactory
import xerial.silk.framework.{ActorService, ScheduleGraph}
import akka.actor.Props


trait TaskDispatcherComponent {

  type TaskDispatcher <: TaskDispatcherAPI
  val taskDispatcher : TaskDispatcher

  val taskDispatcherTimeout = 60

  trait TaskDispatcherAPI {
    def dispatch[A](op:Silk[A])
  }
}


trait TaskDispatcherImpl extends TaskDispatcherComponent {

  type TaskDispatcher = DefaultTaskDispatcher
  val taskDispatcher = new DefaultTaskDispatcher

  class DefaultTaskDispatcher extends TaskDispatcherAPI {

    val logger = LoggerFactory(classOf[TaskDispatcher])

    def dispatch[A](op:Silk[A]) {

      // Create a schedule graph
      val sg = ScheduleGraph(op)
      logger.debug(s"Schedule graph:\n$sg")

      // Launch TaskScheduler and submitter
      for(as <- ActorService.local) {
        val taskQueue = as.actorOf(Props[TaskQueue], name="taskQueue")
        val schedulerRef = as.actorOf(Props(new TaskScheduler(sg)), name="scheduler")

        // Tick scheduler periodically
        import scala.concurrent.duration._
        import as.dispatcher
        as.scheduler.scheduleOnce(taskDispatcherTimeout.seconds){ schedulerRef ! TaskScheduler.Timeout }

        // Start evaluation
        schedulerRef ! TaskScheduler.Start

        // Await termination of the scheduler
        as.awaitTermination()
      }
    }
  }

}



