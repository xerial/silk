package xerial

import xerial.core.log.Logger
import xerial.silk.cluster.Config

/**
 * Helper methods for using Silk. Import this package as follows:
 *
 * {{{
 *   import xerial.silk._
 * }}}
 *
 * @author Taro L. Saito
 */
package object silk extends Logger {

  // TODO setting configurations from SILK_CONFIG file
  /**
   * A global variable for accessing the configurations using `config.get`.
   *
   * This value is shared between thread rather than stored in thread-local storage
   */
  @volatile private[silk] var _config : Config = Config()

  def config = _config

  /**
   * Switch the configurations within the given function block
   * @param c
   * @param f
   * @tparam U
   * @return
   */
  def withConfig[U](c:Config)(f: => U) : U = {
    debug(s"Switch the configuration: $c")
    val prev = _config
    try {

      _config = c
      f
    }
    finally
      _config = prev
  }

}
