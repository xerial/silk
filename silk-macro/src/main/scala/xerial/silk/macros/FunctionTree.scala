//--------------------------------------
//
// FunctionTree.scala
// Since: 2013/05/01 10:19 AM
//
//--------------------------------------

package xerial.silk.macros
import scala.reflect.macros.Context
import scala.language.experimental.macros
import xerial.core.log.Logger
import scala.reflect.runtime.{universe=>ru}
import scala.tools.reflect.ToolBox


/**
 * @author Taro L. Saito
 */
object FunctionTree extends Logger {

  def newSilkMonad[A, B](fExpr:String) : SilkMonad[B] = new SilkMonad[B](fExpr)


  def mapImpl[A:c.WeakTypeTag, B:c.WeakTypeTag](c:Context)(f:c.Expr[A=>B]) = {
    import c.universe._
    val expr = c.Expr[String](Literal(Constant(showRaw(f))))
    reify{ newSilkMonad[A, B]( expr.splice ) }
  }
}


class SilkIntSeq(v:Seq[Int]) {
  def map[B](f: Int => B) : SilkMonad[B] = macro FunctionTree.mapImpl[Int, B]
}

class SilkMonad[A](val expr:String) {

  def tree : ru.Tree = {
    val tb = scala.reflect.runtime.currentMirror.mkToolBox()
    tb.parse(expr).asInstanceOf[ru.Tree]
  }

  def map[B](f: A => B) : SilkMonad[B] = macro FunctionTree.mapImpl[A, B]
}
