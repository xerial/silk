//--------------------------------------
//
// RequestDispatcher.scala
// Since: 2013/07/16 12:18 PM
//
//--------------------------------------

package xerial.silk.webui

import javax.servlet._
import xerial.core.log.Logger
import javax.servlet.http.HttpServletRequest
import xerial.core.io.Resource
import xerial.lens.ObjectSchema
import java.lang.reflect.{Modifier, Method}

/**
 * @author Taro L. Saito
 */
class RequestDispatcher extends Filter with Logger {

  def init(filterConfig: FilterConfig) {
    info(s"Initialize the request dispatcher")
    // Initialize the URL mapping

    def isPublic(m:Method) = {
      Modifier.isPublic(m.getModifiers)
    }
    def isVoid(m:Method) = {
      m.getReturnType == Void.TYPE
    }
    def findClass(name:String) : Option[Class[_]] = {
      try
        Some(Class.forName(name))
      catch {
        case e:Exception => None
      }
    }

    def componentName(path: String): Option[String] = {
      val dot: Int = path.lastIndexOf(".")
      if (dot <= 0)
        None
      else
        Some(path.substring(0, dot).replaceAll("/", "."))
    }

    // Search webui.app package for WebAction classes
    val packagePath = "xerial.silk.webui.app"
    val rl = Resource.listResources(packagePath).filter(_.logicalPath.endsWith(".class"))

    // Find public methods that return nothing (void return type)
    for{
      resource <- rl
      componentName <- componentName(resource.logicalPath)
      cls <- findClass(s"${packagePath}.${componentName}") if classOf[WebAction].isAssignableFrom(cls)
      method <- ObjectSchema.methodsOf(cls) if isPublic(method.jMethod) && isVoid(method.jMethod)
    } {

      info(s"found an action method: ${method}")
      for(pathAnnotation <- method.findAnnotationOf[path]) {
        info(s"has path annotation: ${pathAnnotation}")
      }


    }
  }


  def doFilter(request: ServletRequest, response: ServletResponse, chain: FilterChain) {
    val req = request.asInstanceOf[HttpServletRequest]

    info(s"filter: ${req.getRequestURI}")

    chain.doFilter(req, response)
  }


  def destroy() {

  }
}