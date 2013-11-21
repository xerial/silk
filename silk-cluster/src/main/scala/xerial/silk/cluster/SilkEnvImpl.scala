//--------------------------------------
//
// SilkEnvImpl.scala
// Since: 2013/07/25 6:00 PM
//
//--------------------------------------

package xerial.silk.cluster

import xerial.silk.{SilkEnv, Silk}
import akka.actor.ActorSystem
import scala.reflect.ClassTag
import xerial.silk.cluster.store.DataServer


/**
 * SilkEnv is an entry point to Silk functionality.
 */
class SilkEnvImpl(@transient val zk : ZooKeeperClient,
                  @transient val actorSystem : ActorSystem,
                  @transient val dataServer : DataServer) extends SilkEnv { thisEnv =>

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


  private[silk] def runF0[R](locality:Seq[String], f: => R) = {
    val task = service.localTaskManager.submit(service.classBox.classBoxID, locality)(f)
    // TODO retrieve result
    null.asInstanceOf[R]
  }
}



