 //--------------------------------------
//
// SilkEnv.scala
// Since: 2013/06/22 15:22
//
//--------------------------------------

package xerial.silk.cluster.framework

import xerial.silk.cluster.DataServer
import xerial.silk.framework._
import xerial.silk.framework.ops.{RemoteSeq, SilkSeq}
import java.util.UUID
import java.net.URL
import xerial.core.io.IOUtil
import xerial.core.log.Logger
import xerial.silk.framework.ops.RemoteSeq
import xerial.silk.SilkException


case class RemoteSlice[A](id:UUID, override val nodeName:String, override val index:Int) extends Slice[A](nodeName, index) {
  def data = {


    // TODO download data from remote

    null
  }
}

/**
 * @author Taro L. Saito
 */
trait SilkEnv extends IDUtil with Logger {
  self: LocalTaskManagerComponent with TaskMonitorComponent =>

  import xerial.silk.cluster._

  val dataServer:DataServer

  def newSilk[A](seq:Seq[A]) : SilkSeq[A] = {
    // Register a data to a local data server
    val id = UUID.randomUUID()
    dataServer.registerData(id.toString, seq)

    val url = s"http://${localhost.address}:${dataServer.port}/data/${id.toString}"

    // Let a remote node download the data
    val task = localTaskManager.submit {
      debug("Download data from $url")
      IOUtil.readFully(new URL(url).openStream()) { data =>
        SilkClient.client.map { c =>
          val ds = c.dataServer
          ds.register(id.toString, data)
        }
      }
    }
    val result = for(status <- taskMonitor.completionFuture(task.id)) yield {
      status match {
        // TODO retrieve FContext
        case TaskFinished(node) => RemoteSeq(null, IndexedSeq(RemoteSlice(id, node, 0)))
        case _ => SilkException.error("failed to create data")
      }
    }
    result.get
  }



}