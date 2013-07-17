//--------------------------------------
//
// SilkWebService.scala
// Since: 2013/07/17 12:53 PM
//
//--------------------------------------

package xerial.silk.webui

import xerial.silk.io.ServiceGuard
import org.mortbay.jetty.{Handler, Server}
import org.mortbay.jetty.nio.SelectChannelConnector
import org.mortbay.jetty.handler.{DefaultHandler, HandlerList, ResourceHandler}
import org.mortbay.jetty.webapp.WebAppContext


object SilkWebService {

  def apply(port:Int) : ServiceGuard[SilkWebService] = {
    new ServiceGuard[SilkWebService] {
      def close { service.close }
      protected val service = new SilkWebService(port)
    }
  }

}


/**
 * @author Taro L. Saito
 */
class SilkWebService(port:Int) {

  private val server = new Server()

  {
    val connector = new SelectChannelConnector
    connector.setPort(port)
    server.addConnector(connector)

    val ctx = new WebAppContext()
    ctx.setContextPath("/")
    ctx.setWar("")

    val resourceHandler = new ResourceHandler()
    resourceHandler.setResourceBase(".")

    val handlers = Array[Handler](resourceHandler, new DefaultHandler)
    server.setHandlers(handlers)



    server.start()
  }



  def close {
    server.stop()
    server.join()
  }

}