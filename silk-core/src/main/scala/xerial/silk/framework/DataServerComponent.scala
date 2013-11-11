package xerial.silk.framework

/**
 * @author Taro L. Saito
 */
trait DataServerComponent {
  self: SilkFramework =>

  def dataServer : DataServer

}
