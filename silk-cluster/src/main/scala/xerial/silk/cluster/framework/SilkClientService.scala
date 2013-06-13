//--------------------------------------
//
// SilkClientService.scala
// Since: 2013/06/12 16:21
//
//--------------------------------------

package xerial.silk.cluster.framework

import xerial.silk.cluster._
import xerial.silk.framework._


/**
 * @author Taro L. Saito
 */
trait SilkClientService
  extends SilkFramework
  with DistributedCache
  with ClusterNodeManager
  with ZooKeeperService
  with DefaultConsoleLogger
  with LifeCycle
{

  val host: Host
  val zk: ZooKeeperClient
  val dataServer: DataServer
  val leaderSelector:SilkMasterSelector

//  type Session = SilkSession
//  def run[A](session: Session, op: Silk[A]) = {
//    // TODO impl
//
//  }

  abstract override def startUp {
    info("SilkClientService start up")
    super.startUp
  }

  abstract override def tearDown {
    info("SilkClientService tear down")
    super.tearDown
  }

}

trait SilkMasterService
  extends SilkFramework
  with ClusterResourceManager
  with ZooKeeperService
  with DefaultConsoleLogger
{


}

