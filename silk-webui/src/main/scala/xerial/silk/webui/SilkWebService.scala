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
import java.io.File
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.webapp.WebAppContext
import org.eclipse.jetty.util.resource.ResourceCollection

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

  private var server : Server = null

  {
    info(s"Starting SilkWebService port:$port")
    xerial.silk.cluster.configureLog4j
    server = new Server(port)
    // Use eclipse jdt compiler for compiling JSP pages
    trace(s"JAVA_HOME:${System.getenv("JAVA_HOME")}")
    System.setProperty("org.apache.jasper.compiler.disablejsr199", "true")

    // Read webapp contents inside silk-webui.jar
    val webapp = Resource.find("/xerial/silk/webui/webapp")
    if(webapp.isEmpty)
      throw new IllegalStateException("xerial.silk.webui.webapp is not found")
    val webappResource = webapp.get.toExternalForm

    val ctx = new WebAppContext()
    ctx.setContextPath("/")
    ctx.setExtractWAR(false)
    val localGWTFolder = new File("silk-webui/target/gwt")
    if(localGWTFolder.exists()) {
      val rc = new ResourceCollection(Array(webappResource, localGWTFolder.getAbsolutePath))
      ctx.setBaseResource(rc)
    }
    else
      ctx.setResourceBase(webappResource)

    ctx.setClassLoader(Thread.currentThread.getContextClassLoader)

    server.setHandler(ctx)
    server.start()
    info(s"SilkWebService is ready")
  }



  def close {
    server.stop()
    info("Closed SilkWebService")
    server.join()
  }

}