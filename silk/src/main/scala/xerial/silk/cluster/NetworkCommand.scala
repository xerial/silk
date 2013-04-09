//--------------------------------------
//
// NetworkCommand.scala
// Since: 2013/04/08 17:18
//
//--------------------------------------

package xerial.silk.cluster

import xerial.silk.cluster.SilkClient.{DownloadDataFrom}
import xerial.core.io.IOUtil

/**
 * @author Taro L. Saito2
 */
trait NetworkCommand

case class Scatter(data:Array[Byte], hosts:Seq[String]) extends NetworkCommand



object NetworkCommand {

  def execute(cmd:NetworkCommand) {

    cmd match {
      case Scatter(data, hosts) =>
        // Split data
        val M = hosts.size
        val split : Iterator[Array[Byte]] = data.sliding(M, M)

        // scatter

        // TODO impl
        val client : SilkClient = null

        val dataList = (for(s <- split) yield {
          val dataSplitID = s.hashCode().toString
          (dataSplitID, s)
        }).toArray

        for((dataSplitID, s) <- dataList) {
          // Register the data to the DataServer of the local SilkClient
          client.dataServer.registerData(dataSplitID, s)
        }

        // (data holder's host name, splitID)
        for((h, i) <- hosts.zipWithIndex; remoteClient <- SilkClient.remoteClient(Host(h))) {
          remoteClient ! DownloadDataFrom(client.host, client.dataServer.port, dataList(i)._1)

        }


    }


  }

}
