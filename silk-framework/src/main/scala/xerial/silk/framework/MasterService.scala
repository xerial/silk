package xerial.silk.framework

import xerial.silk.framework.scheduler.TaskScheduler.Task

/**
 * @author Taro L. Saito
 */
trait MasterService {

   type Master <: MasterAPI

   val master : Master

   trait MasterAPI {
     def submitTask[A](task:Task[A]) : Unit
   }

 }
