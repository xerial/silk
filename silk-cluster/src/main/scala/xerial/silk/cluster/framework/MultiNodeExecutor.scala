//--------------------------------------
//
// MultiNodeExecutor.scala
// Since: 2013/06/11 19:09
//
//--------------------------------------

package xerial.silk.cluster.framework

import xerial.silk.framework.{SilkFramework, InMemoryStageManager, ExecutorComponent}

/**
 * @author Taro L. Saito
 */
trait MultiNodeExecutor
  extends ExecutorComponent
  with InMemoryStageManager
  with RunLogger
{

}

trait RunLogger extends SilkFramework {

  abstract override def run[A](silk: Silk[A]) : Result[A] = {
    debug(s"run $silk")
    val result = super.run(silk)
    debug(s"result: $result")
    result
  }
}

