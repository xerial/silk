package xerial.silk.framework.scheduler

import xerial.core.log.Logger
import xerial.silk.{SilkUtil, Silk}
import xerial.silk.framework.scheduler.TaskScheduler.Task
import java.util.UUID
import xerial.silk.cluster.closure.ClosureCleaner
import xerial.silk.cluster.MasterService


/**
 * Evaluate a given silk operation.
 *
 * @author Taro L. Saito
 */
trait EvaluatorComponent
  extends MasterService
{

  def evaluator:EvaluatorAPI

  trait EvaluatorAPI extends Logger {

    def eval[A](classBoxID:UUID, op:Silk[A]) = {

      // Static optimization
      debug(s"Apply static optimization to ${op}")
      val staticOptimizers = TaskScheduler.defaultStaticOptimizers
      val optimized = staticOptimizers.foldLeft[Silk[_]](op){(op, optimizer) => optimizer.optimize(op)}

      // Cleanup closures
      val clean = ClosureCleaner.clean(optimized)

      // Creat a new task
      val task = Task(SilkUtil.newUUID, classBoxID, clean)

      // Send a task request to the master
      master.submitTask(task)
    }
  }
}
