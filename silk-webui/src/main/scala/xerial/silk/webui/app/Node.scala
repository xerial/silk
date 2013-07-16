//--------------------------------------
//
// Node.scala
// Since: 2013/07/16 2:05 PM
//
//--------------------------------------

package xerial.silk.webui.app

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import xerial.silk.webui.{path, WebAction}
import xerial.core.log.Logger

/**
 * @author Taro L. Saito
 */
class Node extends WebAction with Logger {

  @path("/$node/status")
  def status(node:String) {

    info(s"list status of $node")

  }

  def list {
    info(s"list server status")

  }

}