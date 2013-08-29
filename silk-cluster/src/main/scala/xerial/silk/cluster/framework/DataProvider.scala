//--------------------------------------
//
// DataProvider.scala
// Since: 2013/06/22 15:22
//
//--------------------------------------

package xerial.silk.cluster.framework

import xerial.silk.framework._
import xerial.silk.framework.ops.RawSeq
import xerial.silk.{Silk, SilkException}
import xerial.silk.core.SilkSerializer
import xerial.core.log.{LoggerFactory, Logger}
import xerial.silk.cluster.{DataServerComponent, SilkClient, DataServer}
import xerial.core.io.IOUtil
import java.net.URL
import xerial.silk.util.ThreadUtil.ThreadManager
import java.util.UUID


/**
 * Sending local data to a cluster
 *
 * @author Taro L. Saito
 */
trait DataProvider extends IDUtil with Logger {
  self: LocalTaskManagerComponent
    with SliceStorageComponent
    with TaskMonitorComponent
    with ClassBoxComponent
    with DataServerComponent
    with LocalClientComponent =>

  /**
   * Scatter the data to several nodes
   * @param rs
   * @param numSplit
   * @tparam A
   */
  def scatterData[A](rs: RawSeq[A], numSplit: Int = 1) {

    info(s"Registering data: [${rs.idPrefix}] ${rs.fc}")

    if (sliceStorage.getStageInfo(rs).isDefined) {
      warn(s"[${rs.idPrefix}] ${rs.fc} is already registered")
      return
    }

    // Slice width
    val w = (rs.in.size + (numSplit - 1)) / numSplit
    try {
      // Set SliceInfo first to tell the subsequent tasks how many splits exists
      sliceStorage.setStageInfo(rs, StageInfo(-1, numSplit, StageStarted(System.currentTimeMillis())))

      val cbid = classBoxID

      val submittedTasks = for (i <- (0 until numSplit)) yield {
        // Seq might not be serializable, so we translate it into IndexedSeq, which uses serializable Vector class.
        // TODO Send Range without materialization
        // TODO Send large data
        val split = rs.in.slice(w * i, math.min(w * (i + 1), rs.in.size)).toIndexedSeq
        //val serializedSeq = SilkSerializer.serializeObj(split)

        // Register a data to a local data server
        val dataAddress = new URL(s"http://${xerial.silk.cluster.localhost.address}:${dataServer.port}/data/${rs.idPrefix}/$i")
        trace(s"scatter data addresss: $dataAddress")
        dataServer.registerData(s"${rs.idPrefix}/$i", split)

        // Let a remote node have the split
        val task = localTaskManager.submit(DownloadTask(UUID.randomUUID, cbid, rs.id, dataAddress, i, Seq.empty))
        task
      }

      // Await task completion to keep alive the DataServer
//      for (task <- submittedTasks) {
//        // TODO timeout when remote task has no response
//        for (status <- taskMonitor.completionFuture(task.id)) {
//          status match {
//            case TaskFinished(node) =>
//              debug(s"registration finished at $node: ${rs.idPrefix}")
//            case TaskFailed(node, message) =>
//              SilkException.error(s"registration failed at $node: $message")
//            case _ =>
//          }
//        }
//      }
//      sliceStorage.setStageInfo(rs, StageInfo(0, numSplit, StageFinished(System.currentTimeMillis())))
    }
    catch {
      case e: Exception =>
        error(e)
        sliceStorage.setStageInfo(rs, StageInfo(0, numSplit, StageAborted(e.getMessage, System.currentTimeMillis)))
    }



  }

}