//--------------------------------------
//
// Task.scala
// Since: 2013/07/16 5:35 PM
//
//--------------------------------------

package xerial.silk.webui.app

import xerial.silk.webui.WebAction

/**
 * @author Taro L. Saito
 */
class Task extends WebAction {

  def list {
    setContent(s"Task list")
    render
  }
}