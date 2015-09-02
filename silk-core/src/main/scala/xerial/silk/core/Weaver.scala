package xerial.silk.core

/**
 * Weaver is an interface for evaluating Silk operations.
 * InMemoryWeaver, LocalWeaver, ClusterWeaver
 *
 * @author Taro L. Saito
 */
trait Weaver extends Serializable {

  /**
   * Custom configuration type that is specific to the Weaver implementation
   * For example, if one needs to use local weaver, only the LocalConfig type will be set to this type.
   */
  type Config

  /**
   * Configuration object
   */
  val config: Config



}



