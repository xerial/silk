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


case class RemoteSlice[A](id:UUID, override val nodeName:String, override val index:Int) extends Slice[A](nodeName, index) {
  def data = {


    // TODO download data from remote

    null
  }
}

case class NewEnv(zk:ZooKeeperClient) {


}


//object SilkEnv {
//  def newEnv[U](zkc:ZooKeeperClient)(body: => U) = {
//
//    val nodeManager = new ClusterNodeManager with ZooKeeperService {
//      val zk = zkc
//    }.nodeManager
//
//    val node = nodeManager.randomNode
//
//
//  }
//
//}


/**
 * Sending local data to a cluster
 *
 * @author Taro L. Saito
 */
trait DataProvider extends IDUtil with Logger {
  self: LocalTaskManagerComponent with TaskMonitorComponent =>

  import xerial.silk.cluster._

  def convertToRemoteSeq[A](rs:RawSeq[A]) : SilkSeq[A] = {
    // Register a data to a local data server
    // Seq might not be serializable, so we translate it into IndexedSeq, which uses serializable Vector class.
    val id = UUID.randomUUID
    val serializedSeq = SilkSerializer.serializeObj(rs.in.toIndexedSeq)

    // Let a remote node have the data
    val task = localTaskManager.submit {
      val data = serializedSeq
      SilkClient.client.map { c =>
        val ds = c.dataServer
        // Register the serialized data to the data server
        require(id != null, "id must not be null")
        ds.register(id.toString, data)
      }
    }

    // Await task completion
    val result = for(status <- taskMonitor.completionFuture(task.id)) yield {
      status match {
        case TaskFinished(node) =>
          RemoteSeq(rs.fc, IndexedSeq(RemoteSlice(id, node, 0)))
        case _ => SilkException.error("failed to create data")
      }
    }
    result.get
  }

}