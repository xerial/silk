package xerial.silk.cluster.store

import xerial.silk.weaver.Weaver


/**
 * @author Taro L. Saito
 */
trait DataServerComponent {
  self: Weaver =>

  def dataServer : DataServer

}
