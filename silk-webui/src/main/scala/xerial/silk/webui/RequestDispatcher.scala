//--------------------------------------
//
// RequestDispatcher.scala
// Since: 2013/07/16 12:18 PM
//
//--------------------------------------

package xerial.silk.webui

import javax.servlet._
import xerial.core.log.Logger
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}
import xerial.core.io.Resource
import xerial.lens._
import java.lang.reflect.{Modifier, Method}
import scala.language.existentials
import scala.Some
import xerial.lens.MethodParameter

object RequestDispatcher {

  /**
   * Base trate for matching each path component
   */
  sealed trait PathPattern {
    def isValid(value:String) : Boolean
  }
  case class VarBind(name:String, param:MethodParameter) extends PathPattern {
    def isValid(value: String) = {
      TypeConverter.convert(value, param.valueType) match {
        case Some(x) => true
        case None => false
      }
    }
  }
  case class PathMatch(name:String) extends PathPattern {
    def isValid(value: String) = name == value
  }

  case class WebActionMapping(name:String, appCls:Class[_], methodMappings:Seq[MethodMapping]) {
    def findMapping(pathComponents:Seq[String]) = {
      val m = methodMappings.find(m => m.isValid(pathComponents))
      m.map(_.createMapping(pathComponents)).map((m.get, _))
    }
  }

  /**
   * Map path patterns to methods in a WebAction
   * @param pattern
   * @param actionMethod
   */
  case class MethodMapping(pattern:Seq[PathPattern], actionMethod:ObjectMethod) {
    def name = actionMethod.name
    def isValid(pathComponents:Seq[String]) : Boolean = {
      if(pathComponents.size != pattern.size)
        false
      else
        pathComponents.zip(pattern).forall{ case (pc:String, pat:PathPattern) => pat.isValid(pc) }
    }
    def createMapping(pathComponents:Seq[String]) = {
      val result = pathComponents.zip(pattern).collect{
        case (pc, VarBind(name, param)) => param -> TypeConverter.convert(pc, param.valueType).get
      }
      result
    }
  }
}


/**
 * @author Taro L. Saito
 */
class RequestDispatcher extends Filter with Logger {

  import RequestDispatcher._

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
    } {

      val appName = cls.getSimpleName.toLowerCase

      val methodMappers = for(method <- ObjectSchema.methodsOf(cls) if isPublic(method.jMethod) && isVoid(method.jMethod)) yield {
        info(s"found an action method: ${method}")
        val pathAnnotation = method.findAnnotationOf[path]
        if(pathAnnotation.isDefined)  {
          val patterns = for(pc <- splitComponent(pathAnnotation.get.value)) yield {
            if(pc.startsWith("$")) {
              val valName = pc.stripPrefix("$")
              val valType = method.params.find(p => p.name == valName)
              if(valType.isEmpty)
                throw new IllegalArgumentException(s"no param ${valName} is found in method $method")
              VarBind(pc.stripPrefix("$"), valType.get)
            }
            else
              PathMatch(pc)
          }
          MethodMapping(patterns, method)
        }
        else
          MethodMapping(Seq(PathMatch(method.name.toLowerCase)), method)
      }
      mappingTable += appName -> WebActionMapping(appName, cls, methodMappers)
    }
  }

  val mappingTable = collection.mutable.Map[String, WebActionMapping]()


  private def splitComponent(p:String) = p.stripPrefix("/").split("/")

  def doFilter(request: ServletRequest, response: ServletResponse, chain: FilterChain) {
    val req = request.asInstanceOf[HttpServletRequest]
    val res = response.asInstanceOf[HttpServletResponse]
    info(s"filter: ${req.getRequestURI}")

    val path = req.getRequestURI
    val pc = splitComponent(path)

    /**
     * Set servlet parameters to the WebAction class
     * @param appCls
     * @return
     */
    def prepareApp(appCls:Class[_]) = {
      info(s"app class: $appCls")
      val app = appCls.newInstance
      val m = appCls.getDeclaredMethod("init", classOf[HttpServletRequest], classOf[HttpServletResponse])
      m.invoke(app, req, res)
      app.asInstanceOf[AnyRef]
    }

    // Path examples:
    //  1. /<action name>/<method name>?p1=v1&...
    //  2. /<action name>/(<val bind>|<path name>/)+?p1=v1&p2=v2
    if(pc.length >= 2) {
      val appName = pc(0).toLowerCase
      for{
        am @ WebActionMapping(name, appCls, matchers) <- mappingTable.get(appName)
        (action, mapping) <- am.findMapping(pc.drop(1))
      }
      {
        val app = prepareApp(appCls)
        info(s"found mapping: $name/${action.name}, $mapping")

        val mb = new MethodCallBuilder(action.actionMethod, app)
        for((param:MethodParameter, value) <- mapping) {
          mb.set(param.name, value)
        }

        mb.execute
        return
      }
    }

    chain.doFilter(req, response)
  }


  def destroy() {

  }
}