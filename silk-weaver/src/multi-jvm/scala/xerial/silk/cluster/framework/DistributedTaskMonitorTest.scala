//--------------------------------------
//
// DistributedTaskMonitorTest.scala
// Since: 2013/06/14 9:09
//
//--------------------------------------

package xerial.silk.cluster.framework

import xerial.silk.cluster._
import xerial.silk.framework._
import java.util.UUID
import xerial.silk.framework.scheduler.{TaskFinished, TaskStarted}


/**
 * @author Taro L. Saito
 */
object DistributedTaskMonitorTest {

  def syncStatus = "TaskMonitor should synchronize status"


  val taskID = UUID.nameUUIDFromBytes(Array[Byte](1, 3, 4))

}

import DistributedTaskMonitorTest._

class DistributedTaskMonitorTestMultiJvm1 extends Cluster3Spec {

  syncStatus in {
    start { client =>
      debug(s"write status: $taskID")
      client.taskMonitor.setStatus(taskID, TaskStarted(nodeName))

      enterBarrier("taskStarted")
    }
  }

}

class DistributedTaskMonitorTestMultiJvm2 extends Cluster3Spec with Tasks {
  syncStatus in {
    start { client =>
      val f = client.taskMonitor.completionFuture(taskID)
      enterBarrier("taskStarted")
      val status = f.get
      debug(s"task completed: $status")
    }
  }

}

class DistributedTaskMonitorTestMultiJvm3 extends Cluster3Spec with Tasks {
  syncStatus in {
    start { client =>
      enterBarrier("taskStarted")
      client.taskMonitor.setStatus(taskID, TaskFinished(nodeName))
    }
  }

}