//--------------------------------------
//
// Node.scala
// Since: 2013/07/16 2:05 PM
//
//--------------------------------------

package xerial.silk.webui.app

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import xerial.silk.webui.{SilkWebService, path, WebAction}
import xerial.core.log.Logger
import xerial.silk.framework.NodeResourceState

/**
 * @author Taro L. Saito
 */
class Node extends WebAction with Logger {

  def list {

    val nodes = silkClient.hosts.sortBy(_.name)
    val nodeStates = nodes.map {n =>
      n.name -> SilkWebService.service.resourceMonitor.get(n.name)
    }.toMap[String, NodeResourceState]

    val m = silkClient.getMaster

    renderTemplate("nodelist.ssp", Map("hosts"-> nodes, "states" -> nodeStates, "master" -> m.map(_.name).getOrElse("")))
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