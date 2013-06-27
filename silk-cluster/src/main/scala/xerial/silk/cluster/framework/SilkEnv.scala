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
import xerial.silk.cluster.DataServer
import xerial.core.io.IOUtil
import java.net.URL


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

    // Prepare a data server
    for(ds <- DataServer(IOUtil.randomPort)) {

      // Slice width
      val w = (rs.in.size + (numSplit - 1)) / numSplit
      try {
        // Set SliceInfo first to tell the subsequent tasks how many splits exists
        sliceStorage.setStageInfo(rs, StageInfo(numSplit, StageStarted(System.currentTimeMillis())))


        val submittedTasks = for(i <- 0 until numSplit) yield {
          // Seq might not be serializable, so we translate it into IndexedSeq, which uses serializable Vector class.
          // TODO Send Range without materialization
          // TODO Send large data
          val split = rs.in.slice(w * i, math.min(w * (i+1), rs.in.size)).toIndexedSeq
          //val serializedSeq = SilkSerializer.serializeObj(split)

          // Register a data to a local data server
          val dataAddress = new URL(s"http://${localClient.address}:${ds.port}/data/${rs.idPrefix}/$i")
          info(s"data address: $dataAddress")
          ds.registerData(s"${rs.idPrefix}/$i", split)

          // Let a remote node have the split
          val task = localTaskManager.submitF1(){ c: LocalClient =>
            require(rs.id != null, "id must not be null")
            require(dataAddress != null, "dataAddress must not be null")
            // Download data from the local data server
            println(s"address: $dataAddress")
            val slice = Slice(c.currentNodeName, i)
            IOUtil.readFully(dataAddress.openStream) { data =>
              c.sliceStorage.put(rs, i, slice, SilkSerializer.deserializeObj(data))
            }
            //val data = SilkSerializer.deserializeObj[Seq[_]](serializedSeq)
            // Register the serialized data to the data server
            //println("in data distribute task")
            //println(s"register slice $slice")
          }
          task
        }

        // Await task completion
        var failed = false
        try {
          for(task <- submittedTasks) {
            for(status <- taskMonitor.completionFuture(task.id)) {
              status match {
                case TaskFinished(node) =>
                //println(s"registration finished at $node: ${rs.idPrefix}")
                case TaskFailed(node, message) =>
                  sliceStorage.setStageInfo(rs, StageInfo(numSplit, StageAborted(s"registration failed at $node: $message", System.currentTimeMillis)))
                  failed = true
                  SilkException.error("failed to create data")
                case _ =>
              }
            }
          }
        }
        finally {
          if(!failed)
            sliceStorage.setStageInfo(rs, StageInfo(numSplit, StageFinished(System.currentTimeMillis())))
        }
      }
      catch {
        case e:Exception =>
          sliceStorage.setStageInfo(rs, StageInfo(numSplit, StageAborted(s"registration failed ${e.getMessage}", System.currentTimeMillis)))
      }
    }

  }

}