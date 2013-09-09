//--------------------------------------
//
// SilkWebService.scala
// Since: 2013/07/17 12:53 PM
//
//--------------------------------------

package xerial.silk.webui

import xerial.silk.io.ServiceGuard
import xerial.core.io.{IOUtil, Resource}
import xerial.core.log.Logger
import java.io.File
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.webapp.WebAppContext
import org.eclipse.jetty.util.resource.ResourceCollection
import xerial.silk.util.ThreadUtil.ThreadManager
import org.apache.log4j.Level
import org.eclipse.jetty.server.session.HashSessionIdManager
import java.net.{URLClassLoader, URL}

object SilkWebService {



  def apply(port:Int) : ServiceGuard[SilkWebService] = {
    new ServiceGuard[SilkWebService] {
      def close { service.close }
      protected[silk] val service = {
        val ws = new SilkWebService(port)

        // Initialize the top page to invoke compilation of scalate templates
//        val tm = new ThreadManager(1)
//        tm.submit {
//          IOUtil.readFully(new URL(s"http://localhost:$port/").openStream) { data =>
//            // OK
//          }
//        }
//        tm.join
        ws
      }
    }
  }

}


/**
 * @author Taro L. Saito
 */
class SilkWebService(val port:Int) extends Logger {

  private val server : Server = {
    info(s"Starting SilkWebService port:$port")

    //xerial.silk.cluster.configureLog4j

    val server = new Server(port)
    // Set a standard random number generator instead of SecureRandom, which slows down Jetty7 startup.
    val idh = new HashSessionIdManager
    idh.setRandom(new java.util.Random())
    server.setSessionIdManager(idh)

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
    ctx.setAttribute("org.eclipse.jetty.server.webapp.WebInfIncludeJarPattern", ".*/silk.*\\.jar$")

    val localGWTFolder = new File("silk-webui/target/gwt")
    if(localGWTFolder.exists()) {
      // For test-environment
      val rc = new ResourceCollection(Array(webappResource, localGWTFolder.getAbsolutePath))
      ctx.setBaseResource(rc)
    }
    else
      ctx.setResourceBase(webappResource)

    ctx.setParentLoaderPriority(true)
    // Wraps the class loader with URLClassLoader since jetty7 issues an error when using ClasspathFilter,
    // a class loader used in sbt
    val ul = Thread.currentThread().getContextClassLoader match {
      case u:URLClassLoader => u
      case other => new URLClassLoader(Array.empty[URL], other)
    }
    ctx.setClassLoader(ul)

    server.setHandler(ctx)
    server.start()
    info(s"SilkWebService is ready")
    server
  }


  def close {
    server.stop()
    info("Closed SilkWebService")
    server.join()
  }

}