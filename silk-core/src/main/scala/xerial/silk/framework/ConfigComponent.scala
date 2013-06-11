package xerial.silk.framework

/**
 * Configuration
 */
trait ConfigComponent {

  type Config
  def config : Config
}



trait ConfigLoader extends ConfigComponent {




}
