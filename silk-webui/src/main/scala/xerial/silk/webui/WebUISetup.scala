//--------------------------------------
//
// WebUISetup.scala
// Since: 2013/07/16 6:05 PM
//
//--------------------------------------

package xerial.silk.webui

import javax.servlet.{ServletContextEvent, ServletContextListener}
import java.io.File
import xerial.core.log.Logger
import org.apache.log4j.BasicConfigurator

/**
 * @author Taro L. Saito
 */
class WebUISetup extends ServletContextListener with Logger {
  def contextInitialized(e: ServletContextEvent) {
    xerial.silk.cluster.configureLog4j

    trace(s"Initialize the WebUI service")
    val context = e.getServletContext
    val tmpdir = context.getAttribute("javax.servlet.context.tmpdir").asInstanceOf[File]
    WebAction.init(tmpdir)
  }

  def contextDestroyed(e: ServletContextEvent) {

  }
}