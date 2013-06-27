 //--------------------------------------
//
// SilkEnv.scala
// Since: 2013/06/22 15:22
//
//--------------------------------------

package xerial.silk.cluster.framework

import xerial.silk.framework._
import xerial.silk.framework.ops.RawSeq
import xerial.silk.SilkException
import xerial.silk.core.SilkSerializer
import xerial.core.log.Logger


/**
 * Sending local data to a cluster
 *
 * @author Taro L. Saito
 */
trait DataProvider extends IDUtil with Logger {
  self: LocalTaskManagerComponent
    with SliceStorageComponent
    with TaskMonitorComponent
    with LocalClientComponent =>

  def sendToRemote[A](rs:RawSeq[A], numSplit:Int=1) {

    info(s"Registering data: [${rs.idPrefix}] ${rs.fc}")

    if(sliceStorage.getStageInfo(rs).isDefined) {
      warn(s"[${rs.idPrefix}] ${rs.fc} is already registered")
      return
    }

    // Set SliceInfo first to tell the subsequent tasks how many splits exists
    val w = (rs.in.size + (numSplit - 1)) / numSplit

    try {
      sliceStorage.setStageInfo(rs, StageInfo(numSplit, StageStarted(System.currentTimeMillis())))
      // Register a data to a local data server
      val submittedTasks = for(i <- 0 until numSplit) yield {
        // Seq might not be serializable, so we translate it into IndexedSeq, which uses serializable Vector class.
        // TODO Send Range without materialization
        val split = rs.in.slice(w * i, math.min(w * (i+1), rs.in.size)).toIndexedSeq
        val serializedSeq = SilkSerializer.serializeObj(split)

        // Let a remote node have the split
        val task = localTaskManager.submitF1(){ c: LocalClient =>
          val data = SilkSerializer.deserializeObj[Seq[_]](serializedSeq)
          // Register the serialized data to the data server
          //println("in data distribute task")
          require(rs.id != null, "id must not be null")
          val slice = Slice(c.currentNodeName, i)
          //println(s"register slice $slice")
          c.sliceStorage.put(rs, i, slice, data)
        }
        task
      }

      // Await task completion
      for(task <- submittedTasks) {
        for(status <- taskMonitor.completionFuture(task.id)) {
          status match {
            case TaskFinished(node) =>
            //println(s"registration finished at $node: ${rs.idPrefix}")
            case TaskFailed(node, message) =>
              sliceStorage.setStageInfo(rs, StageInfo(numSplit, StageAborted(s"registration failed at $node: $message", System.currentTimeMillis)))
              SilkException.error("failed to create data")
          }
        }
        sliceStorage.setStageInfo(rs, StageInfo(numSplit, StageFinished(System.currentTimeMillis())))
      }
    }
    catch {
      case e:Exception =>
        sliceStorage.setStageInfo(rs, StageInfo(numSplit, StageAborted(s"registration failed ${e.getMessage}", System.currentTimeMillis)))
    }


  }

}