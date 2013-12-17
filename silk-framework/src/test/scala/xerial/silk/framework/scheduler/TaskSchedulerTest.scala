//--------------------------------------
//
// TaskSchedulerTest.scala
// Since: 2013/10/22 4:22 PM
//
//--------------------------------------

package xerial.silk.framework.scheduler

import xerial.silk.util.SilkSpec
import xerial.silk.{SilkEnv, Silk}
import xerial.silk.framework.memory.{InMemory, InMemoryMasterService}
import xerial.silk.framework.{SilkFramework}

/**
 * @author Taro L. Saito
 */
class TaskSchedulerTest extends SilkSpec {
  import Silk._


  def eval(op:Silk[_]) = {
    val t = new EvaluatorComponent
      with SilkFramework
      with InMemoryMasterService
    {
      type Config = MyConfig
      val config = MyConfig()

      case class MyConfig()

      override val taskDispatcherTimeout = 3
      def evaluator = new EvaluatorAPI {

      }
    }

    t.evaluator.eval(op)
  }

  implicit val silk = InMemory.framework

  "TaskScheduler" should {
    "find eligible nodes" in {
      val in = Seq(0, 1, 2, 3, 4, 5).toSilk
      val e = in.map(_+1).map(_*2).filter(_<3)

      eval(e)
    }
  }
}