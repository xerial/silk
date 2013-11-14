package xerial.silk.cluster.store

import xerial.silk.framework.SilkFramework

/**
 * @author Taro L. Saito
 */
trait DataServerComponent {
  self: SilkFramework =>

  def dataServer : DataServer

}
