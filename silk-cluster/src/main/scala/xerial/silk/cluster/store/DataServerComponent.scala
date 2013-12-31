package xerial.silk.cluster.store

import xerial.silk.Weaver


/**
 * @author Taro L. Saito
 */
trait DataServerComponent {
  self: Weaver =>

  def dataServer : DataServer

}
