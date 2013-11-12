//--------------------------------------
//
// InMemoryService.scala
// Since: 2013/11/12 9:25
//
//--------------------------------------

package xerial.silk.framework.memory

import xerial.core.log.LoggerFactory
import xerial.silk.framework.MasterService
import xerial.silk.framework.scheduler.TaskDispatcherImpl
import xerial.silk.framework.scheduler.TaskScheduler.Task

/**
 * @author Taro L. Saito
 */
object InMemoryService {

}


trait InMemoryMasterService
  extends MasterService
  with TaskDispatcherImpl
{

  type Master = MasterAPI

  val master = new MasterAPI {
    val logger = LoggerFactory(classOf[InMemoryMasterService])

    def submitTask[A](task:Task[A]) = {
      logger debug s"received $task"

      taskDispatcher.dispatch(task.op)
    }
  }
}



