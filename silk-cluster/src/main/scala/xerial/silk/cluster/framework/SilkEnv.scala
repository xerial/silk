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


/**
 * Sending local data to a cluster
 *
 * @author Taro L. Saito
 */
trait DataProvider extends IDUtil {
  self: LocalTaskManagerComponent
    with SliceStorageComponent
    with TaskMonitorComponent
    with LocalClientComponent =>

  def sendToRemote[A](rs:RawSeq[A], numSplit:Int=1) {

    // Set SliceInfo first to tell the subsequent tasks how many splits exists
    val w = (rs.in.size + (numSplit - 1)) / numSplit
    sliceStorage.setSliceInfo(rs, SliceInfo(numSplit))

    // Register a data to a local data server
    val submittedTasks = for(i <- 0 until numSplit) yield {
      // Seq might not be serializable, so we translate it into IndexedSeq, which uses serializable Vector class.
      // TODO Send Range without materialization
      val split = rs.in.slice(w * i, math.min(w * (i+1), rs.in.size)).toIndexedSeq
      val serializedSeq = SilkSerializer.serializeObj(split)

      // Let a remote node have the split
      val task = localTaskManager.submit { c: LocalClient =>
        val data = SilkSerializer.deserializeObj[Seq[_]](serializedSeq)
        // Register the serialized data to the data server
        println("in data distribute task")
        require(rs.id != null, "id must not be null")
        val slice = Slice(c.currentNodeName, i)
        println(s"register slice $slice")
        c.sliceStorage.put(rs, i, slice, data)
      }
      task
    }

    // Await task completion
    for(task <- submittedTasks) {
      for(status <- taskMonitor.completionFuture(task.id)) {
        status match {
          case TaskFinished(node) =>
            println(s"registration finished at $node: ${rs.idPrefix}")
          case _ => SilkException.error("failed to create data")
        }
      }
    }


  }

}