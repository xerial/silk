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
    request.setAttribute("content", s"list cluster nodes")
    val dispatcher = request.getRequestDispatcher("/page/content.jsp")
    dispatcher.forward(request, response)
  }

  @path("/$node/status")
  def status(node:String) {

    info(s"Show status of $node")

    request.setAttribute("content", s"status of $node")
    val dispatcher = request.getRequestDispatcher("/page/content.jsp")
    dispatcher.forward(request, response)
  }

  @path("/$node/tasks")
  def tasks(node:String, showCompleted:Boolean=false) {
    info(s"show tasks: show completed:$showCompleted")
  }


}