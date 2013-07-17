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
import xerial.core.io.{IOUtil, Resource}
import java.io.FileOutputStream
import xerial.core.log.Logger


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

  private val server = new Server()

  {
    val connector = new SelectChannelConnector
    connector.setPort(port)
    server.addConnector(connector)


    // Copy webapp resources to a temporary folder
    import xerial.core.io.Path._
    val config = xerial.silk.cluster.config
    val webappDir = config.silkTmpDir / "webui"
    webappDir.mkdirs()
    for(r <- Resource.listResources("xerial.silk.webui.webapp")) {
      val path = webappDir / r.logicalPath
      if(r.isDirectory) {
        trace(s"mkdir: $path")
        path.mkdirs()
      }
      else
        IOUtil.readFully(r.url.openStream) { b =>
          trace(s"Copy resource to $path")
          val fout = new FileOutputStream(path)
          fout.write(b)
          fout.close
        }
    }
    val ctx = new WebAppContext()
    ctx.setContextPath("/")
    ctx.setResourceBase(webappDir.getAbsolutePath)
    ctx.setClassLoader(Thread.currentThread.getContextClassLoader)

    server.addHandler(ctx)
    server.start()
    info("Started SilkWebService")
  }



  def close {
    server.stop()
    info("Closed SilkWebService")
    server.join()
  }

}