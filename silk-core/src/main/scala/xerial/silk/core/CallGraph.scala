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


    a match {
      case FlatMap(prev, f, fExpr) =>
        val st = fExpr.staticType
        debug(st)
        st match {
          case t @ TypeRef(prefix, symbol, List(from, to)) =>
            val inputCl = mirror.runtimeClass(from)
            val z = TypeUtil.zero(inputCl)
            val nextExpr = f.asInstanceOf[Any => Any].apply(z)
            debug(s"next expr: $nextExpr")
          case _ => warn(s"unknown type: ${st}")
        }

      case e => debug(s"traversal is not yet implemented for $e")
    }



    new CallGraph
  }

}

class CallGraph {

}