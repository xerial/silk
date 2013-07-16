//--------------------------------------
//
// WebAction.scala
// Since: 2013/07/16 2:06 PM
//
//--------------------------------------

package xerial.silk.webui

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

/**
 * @author Taro L. Saito
 */
trait WebAction {

  protected def request : HttpServletRequest
  protected def response : HttpServletResponse

}