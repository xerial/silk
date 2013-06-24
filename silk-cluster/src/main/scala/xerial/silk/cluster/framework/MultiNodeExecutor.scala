//--------------------------------------
//
// MultiNodeExecutor.scala
// Since: 2013/06/11 19:09
//
//--------------------------------------

package xerial.silk.cluster.framework

import xerial.silk.framework._
import xerial.core.log.Logger

/**
 * @author Taro L. Saito
 */
trait MultiNodeExecutor
  extends SilkRunner
  with ExecutorComponent
  with SliceStorageComponent
  with InMemoryStageManager
  with RunLogger
{

}

trait RunLogger extends SilkRunner with Logger { 

  abstract override def run[A](session:Session, silk: Silk[A]) : Result[A] = {
    debug(s"run $silk")
    val result = super.run(session, silk)
    debug(s"result: $result")
    result
  }
}

