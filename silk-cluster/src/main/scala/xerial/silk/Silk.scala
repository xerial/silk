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

object SilkEnv {

  private[silk] var actorSystem : ActorSystem  = null
  private[silk] var zk : ZooKeeperClient = null

}


/**
 * @author Taro L. Saito
 */
object Silk {
  def weave[U](block: =>U):U = {
    import xerial.silk.cluster._


    for(zk <- ZooKeeper.defaultZkClient) {
      SilkEnv.zk = zk
      SilkEnv.actorSystem = ActorService.getActorSystem(localhost.address, IOUtil.randomPort)
      block
    }


  }
}