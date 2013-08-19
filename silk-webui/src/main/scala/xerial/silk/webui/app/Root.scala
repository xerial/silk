//--------------------------------------
//
// Root.scala
// Since: 2013/08/20 1:18
//
//--------------------------------------

package xerial.silk.webui.app

import xerial.silk.webui.{path, WebAction}

/**
 * Methods in Root is mapped to http://(web UI address)/(method name)
 *
 * @author Taro L. Saito
 */
class Root extends WebAction {

  @path("/")
  def index {
    renderTemplate("index.ssp")
  }

  @path("/gwt")
  def gwt {
    renderTemplate("gwt.ssp")
  }


}