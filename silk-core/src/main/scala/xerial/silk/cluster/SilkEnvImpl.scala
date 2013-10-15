//--------------------------------------
//
// SilkEnvImpl.scala
// Since: 2013/07/25 6:00 PM
//
//--------------------------------------

package xerial.silk.cluster

import xerial.silk.cluster.framework.ActorService
import xerial.core.io.IOUtil
import xerial.silk.{SilkEnv, Silk}
import akka.actor.ActorSystem
import scala.reflect.ClassTag
import xerial.silk.framework.ops.RawSeq


/**
 * @author Taro L. Saito
 */
object SilkEnvImpl {

  def silk[U](block: => U):U = {
    val result = for{
      zk <- ZooKeeper.defaultZkClient
      actorSystem <- ActorService(localhost.address, IOUtil.randomPort)
      dataServer <- DataServer(IOUtil.randomPort, keepAlive=false)
    } yield {
      val env = new SilkEnvImpl(zk, actorSystem, dataServer)
      Silk.setEnv(env)
      block
    }
    result.head
  }
}


/**
 * SilkEnv is an entry point of Silk functionality.
 */
class SilkEnvImpl(@transient zk : ZooKeeperClient,
                  @transient actorSystem : ActorSystem,
                  @transient dataServer : DataServer) extends SilkEnv { thisEnv =>

  @transient val service = new SilkService {
    val zk = thisEnv.zk
    val actorSystem = thisEnv.actorSystem
    val dataServer = thisEnv.dataServer
  }

  def run[A](silk:Silk[A]) = {
    service.run(silk)
  }
  def run[A](silk: Silk[A], target: String) = {
    service.run(silk, target)
  }

  def eval[A](silk:Silk[A]) = {
    service.eval(silk)
  }

  def sessionFor[A:ClassTag] = {
    import scala.reflect.runtime.{universe => ru}
    import ru._
    val t = scala.reflect.classTag[A]
  }

  def sendToRemote[A](seq: RawSeq[A], numSplit:Int = 1) = {
    service.scatterData(seq, numSplit)
    seq
  }


  private[silk] def runF0[R](locality:Seq[String], f: => R) = {
    val task = service.localTaskManager.submit(service.classBoxID, locality)(f)
    // TODO retrieve result
    null.asInstanceOf[R]
  }
}



