//--------------------------------------
//
// SilkWebService.scala
// Since: 2013/07/17 12:53 PM
//
//--------------------------------------

package xerial.silk.webui

import xerial.silk.io.ServiceGuard
import xerial.core.io.Resource
import xerial.core.log.Logger
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.webapp.WebAppContext


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
class SilkWebService(val port:Int) extends Logger {

  private val server = new Server(port)

  {
    // Use eclipse jdt compiler for compiling JSP pages
    info(s"JAVA_HOME:${System.getenv("JAVA_HOME")}")
    System.setProperty("org.apache.jasper.compiler.disablejsr199", "true")

    // Read webapp contents inside silk-webui.jar
    val webapp = Resource.find("/xerial/silk/webui/webapp")
    if(webapp.isEmpty)
      throw new IllegalStateException("xerial.silk.webui.webapp is not found")
    val ctx = new WebAppContext()
    ctx.setContextPath("/")
    ctx.setResourceBase(webapp.get.toExternalForm)
    ctx.setClassLoader(Thread.currentThread.getContextClassLoader)
    ctx.setParentLoaderPriority(true)

    server.setHandler(ctx)
    server.start()
    info("Started SilkWebService")
  }



  def close {
    server.stop()
    info("Closed SilkWebService")
    server.join()
  }

}