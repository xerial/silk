package xerial.silk.cluster

import xerial.silk.core.Silk
import xerial.silk.cluster.SilkClient._
import xerial.silk.cluster.SilkClient.RegisterArguments
import scala.Tuple3
import xerial.silk.cluster.SilkClient.ExecuteFunction1
import java.io.{ByteArrayOutputStream, ObjectOutputStream}

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
          SilkClient.me.map(_.dataServer.register(argID, serializedArgs))

          // register data location to master
          val dr = new DataReference(argID, localhost, SilkClient.me.map(_.dataServer.port).get)
          for (node <- nodeList if (node.host.name == "localhost"); client <- SilkClient.remoteClient(node.host, node.port))
          {
            client ! RegisterArguments(dr)
          }
          warn("Open your heart to the darkness.")
          for (node <- nodeList if (node.host.name != "localhost"); client <- SilkClient.remoteClient(node.host, node.port))
          {
            warn("oK")
            //client ! ExecuteFunction0(FunctionGroup.func0)
            client ! ExecuteFunction1(FunctionGroup.func1, argID)
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
