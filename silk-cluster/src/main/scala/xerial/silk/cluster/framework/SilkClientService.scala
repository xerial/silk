//--------------------------------------
//
// SilkClientService.scala
// Since: 2013/06/12 16:21
//
//--------------------------------------

package xerial.silk.cluster.framework

import xerial.silk.cluster._
import xerial.silk.framework.{DistributedFramework, SilkFramework, LifeCycle}
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import xerial.silk.util.ThreadUtil.ThreadManager
import xerial.silk.mini.SilkSession


/**
 * @author Taro L. Saito
 */
trait SilkClientService
  extends DistributedCache
  with ZooKeeperService {

  val host: Host
  val zk: ZooKeeperClient
  val dataServer: DataServer
  val leaderSelector:SilkMasterSelector

//  type Session = SilkSession
//  def run[A](session: Session, op: Silk[A]) = {
//    // TODO impl
//
//  }


}

