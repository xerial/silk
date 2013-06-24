//--------------------------------------
//
// Silk.scala
// Since: 2013/06/24 1:38 PM
//
//--------------------------------------

package xerial.silk


import xerial.silk.cluster.{ZooKeeperClient, ZooKeeper}
import xerial.silk.cluster.framework.ActorService
import xerial.core.io.IOUtil
import akka.actor.ActorSystem

/**
 * Global variables
 */
object SilkEnv {

  private[silk] var actorSystem : ActorSystem  = null
  private[silk] var zk : ZooKeeperClient = null

}


/**
 * @author Taro L. Saito
 */
object Silk {
  def silk[U](block: =>U):U = {
    import xerial.silk.cluster._
    val result = for{
      as <- ActorService(localhost.address, IOUtil.randomPort)
      zk <- ZooKeeper.defaultZkClient
    } yield {
      SilkEnv.zk = zk
      SilkEnv.actorSystem = as
      block
    }
    result.head
  }
}