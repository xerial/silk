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

  def list {
    info(s"list cluster nodes")
  }

  @path("/$node/status")
  def status(node:String) {

    info(s"Show status of $node")

    response.setContentType("text/html")
    response.getWriter.println(s"status of $node")

  }

  @path("/$node/tasks")
  def tasks(node:String, showCompleted:Boolean=false) {
    info(s"show tasks: show completed:$showCompleted")
  }


}