package xerial.silk.cluster

import xerial.silk.core.Silk
import xerial.silk.cluster.SilkClient._
import xerial.silk.cluster.SilkClient.RegisterArguments
import xerial.silk.cluster.SilkClient.ExecuteFunction1
import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import xerial.silk.multijvm.Cluster2Spec

object Serializer
{
  def serializeObject(obj: AnyRef): Array[Byte] =
  {
    val baos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(obj)
    oos.close
    baos.toByteArray
  }
}

object FunctionGroup
{
  val func0 = () => println("Yes, my master.")
  val func1 = (num: Int) => println(s"Master! I am No.$num")
}

class BroadcastTestMultiJvm1 extends Cluster2Spec
{
  "start cluster and broadcast data" in
    {
      start
      {
        client =>
          val nodeList = Silk.hosts
          info(s"nodes: ${nodeList.mkString(", ")}")

          // serialize data and get data ID
          val argList = Tuple1(10)
          val serializedArgs = Serializer.serializeObject(argList)
          val argID = serializedArgs.hashCode.toString

          // register data to DataServer in the client of this process
          SilkClient.client.map(_.dataServer.register(argID, serializedArgs))

          // register data location to master
          val dr = new DataReference(argID, localhost, SilkClient.client.map(_.dataServer.port).get)
          for (client <- SilkClient.localClient)
          {
            client ! RegisterArguments(dr)
          }
          warn("Open your heart to the darkness.")
          for (node <- nodeList; client <- SilkClient.remoteClient(node.host, node.port))
          {

            //client ! ExecuteFunction0(FunctionGroup.func0)
            client ! ExecuteFunction1(FunctionGroup.func1, argID, serializedArgs.length)
          }
      }
    }
}

class BroadcastTestMultiJvm2 extends Cluster2Spec
{
  "start cluster and accept data" in
    {
      start(client => {})
    }
}
/*

class BroadcastTestMultiJvm3 extends Cluster3Spec
{
  "start cluster and accept data" in
    {
      start(client => {})
    }
}*/
