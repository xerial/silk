//--------------------------------------
//
// SilkClientService.scala
// Since: 2013/06/12 16:21
//
//--------------------------------------

package xerial.silk.cluster.framework

import xerial.silk.cluster.{ZooKeeperClient, DataServer, SilkClient}
import xerial.silk.framework.{SilkFramework, LifeCycle}
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import xerial.silk.util.ThreadUtil.ThreadManager


/**
 * @author Taro L. Saito
 */
trait SilkClientService extends ZooKeeperService with LifeCycle {

  val zooKeeper: ZooKeeperClient = newZooKeeperConnection
  val dataServer: DataServer

  def service : Unit

  def startUp = {



  }

  def terminate = {


  }

}

