//--------------------------------------
//
// WebAction.scala
// Since: 2013/07/16 2:06 PM
//
//--------------------------------------

package xerial.silk.webui

import org.fusesource.scalate._
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import java.io.File

object WebAction {

  private[webui] var templateEngine = new TemplateEngine

  def init(tmpDir:File) {
    templateEngine.workingDirectory = tmpDir
  }



}

/**
 * @author Taro L. Saito
 */
trait WebAction {

  import WebAction._

  private var _req : HttpServletRequest = null
  private var _res : HttpServletResponse = null

  protected def init(req:HttpServletRequest, res:HttpServletResponse) {
    _req = req
    _res = res
  }


  protected def request : HttpServletRequest = _req
  protected def response : HttpServletResponse = _res


  protected def setContent(content:String) {
    request.setAttribute("content", content)
  }

  protected def render = {
    val dispatcher = request.getRequestDispatcher("/page/content.jsp")
    dispatcher.forward(request, response)
  }

  protected def renderWith(jspPage:String="/page/content.jsp") = {
    val dispatcher = request.getRequestDispatcher(jspPage)
    dispatcher.forward(request, response)
  }


  protected def renderTemplate(templatePath:String, attributes:Map[String,Any]=Map.empty) = {
    val content = templateEngine.layout(s"/xerial/silk/webui/ssp/$templatePath", attributes)
    setContent(content)
    render
  }

}