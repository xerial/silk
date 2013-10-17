package xerial.silk.framework

import xerial.silk.cluster.DataServer

/**
 * @author Taro L. Saito
 */
trait DataServerComponent {
  self: SilkFramework =>

  def dataServer : DataServer

}
