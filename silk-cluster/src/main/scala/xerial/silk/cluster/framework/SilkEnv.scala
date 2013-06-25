 //--------------------------------------
//
// SilkEnv.scala
// Since: 2013/06/22 15:22
//
//--------------------------------------

package xerial.silk.cluster.framework

import xerial.silk.cluster.{ZooKeeperClient, DataServer}
import xerial.silk.framework._
import xerial.silk.framework.ops.{RawSeq, RemoteSeq, SilkSeq}
import java.util.UUID
import java.net.URL
import xerial.core.io.IOUtil
import xerial.core.log.Logger
import xerial.silk.SilkException
import xerial.silk.core.SilkSerializer


//case class RemoteSlice[A](override val nodeName:String, override val index:Int) extends Slice[A](nodeName, index) {




/**
 * Sending local data to a cluster
 *
 * @author Taro L. Saito
 */
trait DataProvider extends IDUtil {
  self: LocalTaskManagerComponent with TaskMonitorComponent with SliceStorageComponent =>

  import xerial.silk.cluster._

  def sendToRemote[A](rs:RawSeq[A]) {
    // Register a data to a local data server
    // Seq might not be serializable, so we translate it into IndexedSeq, which uses serializable Vector class.
    // TODO Send Range without materialization
    val serializedSeq = SilkSerializer.serializeObj(rs.in.toIndexedSeq)

    // Let a remote node have the data
    val task = localTaskManager.submit {
      val data = serializedSeq
      SilkClient.client.map { c =>
        val ds = c.dataServer
        // Register the serialized data to the data server
        require(rs.id != null, "id must not be null")
        val path = rs.id.toString.substring(0, 8) + "/0"
        ds.register(path, data)
        val slice = Slice(c.currentNodeName, 0)
        c.sliceStorage.put(rs, 0, slice)
        c.sliceStorage.setSliceInfo(rs, SliceInfo(1))
        println(s"register slice: $slice")
      }
    }

    // Await task completion
    for(status <- taskMonitor.completionFuture(task.id)) yield {
      status match {
        case TaskFinished(node) =>
          println(s"registration finished at $node: $rs")
        case _ => SilkException.error("failed to create data")
      }
    }
  }

}