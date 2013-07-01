package xerial.silk.cluster

import xerial.silk.cluster.SilkClient._
import xerial.silk.cluster.SilkClient.RegisterData
import java.io.{ByteArrayInputStream, ObjectInputStream}
import java.util.UUID
import xerial.silk.cluster.SilkMaster.{DataHolder, DataNotFound}
import java.net.URL
import xerial.core.io.IOUtil
import scala.util.Random


object Downloader
{
  def dl(dr: DataReference): AnyRef =
  {
    val dataURL = new URL(s"http://${dr.host.address}:${dr.port}/data/${dr.id}")
    IOUtil.readFully(dataURL.openStream)
    {
      data =>
        val ois = new ObjectInputStream(new ByteArrayInputStream(data))
        return ois.readObject
    }
  }

  val download = dl(_)
}

class BroadcastTestMultiJvm1 extends Cluster3Spec
{
  "start cluster and broadcast data" in
    {
      start
      {
        env =>
          // host info
          val nodeList = xerial.silk.cluster.hosts
          info(s"nodes: ${nodeList.mkString(", ")}")

          /* Generate data, serialize it, registerByteData to the data server
             , and send this information to the master. */
          val dataRand = new Random
          val data = Array.fill(1024)(dataRand.nextInt)
          val serializedData = Serializer.serializeObject(data)
          val dataID = UUID.randomUUID.toString
          env.client.dataServer.registerByteData(dataID, serializedData)
          val dataDR = new DataReference(dataID, localhost, SilkClient.client.map(_.dataServer.port).get)
          env.clientActor ! RegisterData(dataDR)

          // Send function to download data
          val serializedDataDR = Serializer.serializeObject(Tuple1(dataDR)) // an argument of downloader function
          val argsID = UUID.randomUUID.toString
          SilkClient.client.map(_.dataServer.registerByteData(argsID, serializedDataDR))
          val argDR = new DataReference(argsID, localhost, SilkClient.client.map(_.dataServer.port).get)
          env.clientActor ! RegisterData(argDR)
          val resultIDs = List.fill(nodeList.length)(UUID.randomUUID.toString)
          for ((node, resID) <- nodeList zip resultIDs; client <- SilkClient.remoteClient(node.host, node.clientPort))
          {
            env.clientActor ! ExecuteFunction1(Downloader.download, argsID, resID)
          }

          // wait while data downloaded
          Thread.sleep(3000)

          // see if the data is correctly downloaded
          for ((node, resID) <- nodeList zip resultIDs; client <- SilkClient.remoteClient(node.host, node.clientPort))
          {
            def getResult: Array[Byte] =
            {
              env.clientActor ? GetDataInfo(resID) match
              {
                case DataHolder(id, holder) =>
                {
                  val dataURL = new URL(s"http://${holder.host.address}:${holder.port}/data/${id}")
                  info(s"Accessing ${dataURL.toString}")
                  IOUtil.readFully(dataURL.openStream){return _}
                }
                case DataNotFound(id) => getResult
              }
            }

            val ois = new ObjectInputStream(new ByteArrayInputStream(getResult))
            val result = ois.readObject

            assert(data === result)
          }
      }
    }
}

class BroadcastTestMultiJvm2 extends Cluster3Spec
{
  "start cluster and accept data" in
    {
      start(env => {})
    }
}

class BroadcastTestMultiJvm3 extends Cluster3Spec
{
  "start cluster and accept data" in
    {
      start(env => {})
    }
}
