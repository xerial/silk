//--------------------------------------
//
// DistributedTaskTest.scala
// Since: 2013/06/14 19:04
//
//--------------------------------------

package xerial.silk.cluster.framework

import xerial.silk.cluster.Cluster3Spec

/**
 * @author Taro L. Saito
 */
object DistributedTaskTest {
  def submitTask = "TaskMonitor should monitor submitted tasks"
}

import DistributedTaskTest._

class DistributedTaskTestMultiJvm1 extends Cluster3Spec {
  submitTask in {
    start { env =>
    // submit a task
      val task = env.client.localTaskManager.submit {
        println("hello world")
      }

      val future = env.client.taskMonitor.completionFuture(task.id)
      val taskStatus = future.get
      info(s"task status: $taskStatus")
      enterBarrier("taskCompletion")
    }
  }
}

class DistributedTaskTestMultiJvm2 extends Cluster3Spec  {

  submitTask in {
    start { env =>

      val task = env.client.localTaskManager.submit {
        println("hello silk cluster")
      }

      val future = env.client.taskMonitor.completionFuture(task.id)
      val taskStatus = future.get
      info(s"task status: $taskStatus")

      enterBarrier("taskCompletion")
    }
  }
}


class DistributedTaskTestMultiJvm3 extends Cluster3Spec  {

  submitTask in {
    start { env =>
      enterBarrier("taskCompletion")
    }
  }

}
