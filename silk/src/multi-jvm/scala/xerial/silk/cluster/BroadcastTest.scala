package scala.xerial.silk.cluster

import xerial.silk.core.Silk
import xerial.silk.cluster._
import xerial.silk.cluster.SilkClient.{GetPort, RegisterArguments, ExecuteFunction1, ExecuteFunction0}
import akka.actor._
import akka.pattern.ask
import scala.concurrent.Await
import akka.util.Timeout
import scala.concurrent.duration._

object FunctionGroup
{
  val func0 = () => println("Yes, my master.")
  val func1 = (num: Int) => println(s"Master! I am No.$num")
}

case class DataReference(id: String, host: Host, port: Int)

class BroadcastTestMultiJvm1 extends Cluster3Spec
{
  "start cluster and broadcast data" in
    {
      start
      {
        val nodeList = Silk.hosts
        info(s"nodes: ${nodeList.mkString(", ")}")

        val argList = Tuple3(10, "hoge", 50.0)


        // register to argument of function ....
        for (me <- SilkClient.localClient)
        {
          val future = me.?(GetPort, 3.seconds)
          Await.result(future, 3.seconds)
          val dr = new DataReference(argList.hashCode.toString, localhost, )
          me ! RegisterArguments(dr)
        }


        println("Open your heart to the darkness.")
        for (node <- nodeList if (node.host != StandaloneCluster.lh); client <- SilkClient.remoteClient(node.host, node.port))
        {
          //client ! ExecuteFunction0(FunctionGroup.func0)


          client ! ExecuteFunction1(FunctionGroup.func1, dr: DataReference)
        }

      }
    }
}

class BroadcastTestMultiJvm2 extends Cluster3Spec
{
  "start cluster and accept data" in
    {
      start()
    }
}

class BroadcastTestMultiJvm3 extends Cluster3Spec
{
  "start cluster and accept data" in
    {
      start()
    }
}