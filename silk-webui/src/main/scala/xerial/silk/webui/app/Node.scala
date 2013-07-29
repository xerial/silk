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

    import xerial.silk.cluster._

    val nodes = hosts.sortBy(_.name)
    val m = master

    renderTemplate("nodelist.ssp", Map("hosts"-> nodes, "master" -> m.map(_.name).getOrElse("")))
  }

  @path("/$node/status")
  def status(node:String) {
    setContent(s"status of $node node!!!")
    render
  }

  @path("/$node/tasks")
  def tasks(node:String, showCompleted:Boolean=false) {
    setContent(s"tasks of $node")
    render
  }


}