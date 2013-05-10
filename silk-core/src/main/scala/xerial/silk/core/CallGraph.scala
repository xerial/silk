//--------------------------------------
//
// CallGraph.scala
// Since: 2013/05/10 4:14 PM
//
//--------------------------------------

package xerial.silk.core

import xerial.core.log.Logger
import xerial.lens.TypeUtil

/**
 * @author Taro L. Saito
 */
object CallGraph extends Logger {

  import SilkFlow._

  import scala.reflect.runtime.{universe=>ru}
  import ru._

  private def mirror = ru.runtimeMirror(Thread.currentThread.getContextClassLoader)

  def apply[A](a:Silk[A]) : CallGraph = {

    val b = new Builder
    b.traverse(a)

    b.build
  }

  private class Builder {

    var cache = Set.empty[Any]

    def traverse(a:Any) {
      if(!cache.contains(a))
        cache += a

      a match {
        case f:SilkFlow[_, _] =>
          debug(s"find ${a.getClass.getSimpleName}")
        case _ =>
      }

      a match {
        case fm @ FlatMap(prev, f, fExpr) =>
          traverse(prev)
          fExpr.staticType match {
            case t @ TypeRef(prefix, symbol, List(from, to)) =>
              val inputCl = mirror.runtimeClass(from)
              val z = TypeUtil.zero(inputCl)
              val nextExpr = f.asInstanceOf[Any => Any].apply(z)
              traverse(nextExpr)
            case other => warn(s"unknown type: ${other}")
          }
        case fm @ MapFun(prev, f, fExpr) =>
          traverse(prev)
          fExpr.staticType match {
            case t @ TypeRef(prefix, symbol, List(from, to)) =>
              val inputCl = mirror.runtimeClass(from)
              val z = TypeUtil.zero(inputCl)
              val nextExpr = f.asInstanceOf[Any => Any].apply(z)
              traverse(nextExpr)
            case other => warn(s"unknown type: ${other}")
          }
        case s @ SaveToFile(prev) =>
          traverse(prev)
        case s @ ShellCommand(sc, args, argExpr) =>
          args.foreach(traverse(_))
        case f:SilkFlow[_, _] =>
          warn(s"not yet implemented ${f.getClass.getSimpleName}")
        case e =>
          // ignore
      }
    }

    def build : CallGraph = new CallGraph

  }


}

class CallGraph {

}