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
import xerial.silk.framework.TaskStarted
import xerial.silk.framework.TaskFinished
import xerial.silk.framework.TaskStarted
import xerial.silk.framework.TaskFinished


/**
 * @author Taro L. Saito
 */
object DistributedTaskMonitorTest {

  def syncStatus = "TaskMonitor should synchronize status"


  def newMonitor(env:SilkEnvImpl) : TaskMonitor = {
    new DistributedTaskMonitor with ZooKeeperService {
      val zk = env.zk
    }.taskMonitor
  }

  val taskID = UUID.nameUUIDFromBytes(Array[Byte](1, 3, 4))

}

import DistributedTaskMonitorTest._

class DistributedTaskMonitorTestMultiJvm1 extends Cluster3Spec {

  syncStatus in {
    start { env =>
      val monitor = newMonitor(env)
      debug(s"write status: $taskID")
      monitor.setStatus(taskID, TaskStarted(nodeName))

      enterBarrier("taskStarted")
    }
  }

}

class DistributedTaskMonitorTestMultiJvm2 extends Cluster3Spec with Tasks {
  syncStatus in {
    start { env =>
      val monitor = newMonitor(env)

      val f = monitor.completionFuture(taskID)
      enterBarrier("taskStarted")
      val status = f.get
      debug(s"task completed: $status")
    }
  }

}

class DistributedTaskMonitorTestMultiJvm3 extends Cluster3Spec with Tasks {
  syncStatus in {
    start { env =>
      val monitor = newMonitor(env)

      enterBarrier("taskStarted")
      monitor.setStatus(taskID, TaskFinished(nodeName))
    }
  }

}