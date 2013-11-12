package xerial.silk.framework.scheduler

import xerial.silk.framework.{ClassBox, ClosureCleaner, MasterService}
import xerial.core.log.Logger
import xerial.silk.Silk
import xerial.silk.framework.scheduler.TaskScheduler.NewTask
import java.util.UUID


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

    def eval[A](classBoxID:UUID, op:Silk[A]) {

      // Static optimization
      debug(s"Apply static optimization to ${op}")
      val staticOptimizers = TaskScheduler.defaultStaticOptimizers
      val optimized = staticOptimizers.foldLeft[Silk[_]](op){(op, optimizer) => optimizer.optimize(op)}

      // Cleanup closures
      val clean = ClosureCleaner.clean(optimized)

      // Send a task request to the master
      master.submitTask(NewTask(classBoxID, clean))
    }
  }
}
