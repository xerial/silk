//--------------------------------------
//
// InMemoryService.scala
// Since: 2013/11/12 9:25
//
//--------------------------------------

package xerial.silk.framework.memory

import xerial.silk.framework.MasterService
import xerial.silk.framework.TaskScheduler.NewTask
import xerial.core.log.LoggerFactory

/**
 * @author Taro L. Saito
 */
object InMemoryService {

}


trait InMemoryMasterService extends MasterService {

  type Master = MasterAPI

  val master = new MasterAPI {
    val logger = LoggerFactory(classOf[InMemoryMasterService])

    def submitTask[A](task:NewTask[A]) = {
      logger debug s"received $task"



    }
  }
}


