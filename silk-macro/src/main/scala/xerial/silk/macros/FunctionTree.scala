//--------------------------------------
//
// FunctionTree.scala
// Since: 2013/05/01 10:19 AM
//
//--------------------------------------

package xerial.silk.macros
import scala.reflect.macros.Context
import scala.language.experimental.macros
import scala.language.existentials
import xerial.core.log.Logger
import scala.reflect.runtime.{universe=>ru}
import scala.tools.reflect.ToolBox


/**
 * @author Taro L. Saito
 */
object FunctionTree extends Logger {

  //def newSilkMonad[A](f:ru.Expr[_]) : SilkMonad[A] = new SilkMonad[A](f)


  def mapImpl[A:c.WeakTypeTag, B:c.WeakTypeTag](c:Context)(f:c.Expr[A=>B]) = {
    import c.universe._
    val v = Literal(Constant(showRaw(f)))
    c.Expr[SilkMonad[B]](Apply(Select(reify{SilkMonad}.tree, newTermName("apply")), List(c.prefix.tree, v)))
  }

}

import ru._

case class FunCall(valName:String, body:Tree)

object FunCall {
  def unapply(t:Tree) : Option[FunCall] = {
    t match {
      case Apply(Ident(func), List(Apply(h, List(BindToVal(valBind))), body)) if func.decoded == "Function"
      => Some(FunCall(valBind.toString, body))
      // Function call
      case _ => None
    }
  }

}

object BindToVal {


  def unapply(t:Tree) : Option[String] = {
    t match {
      case Apply(Ident(name), List(Apply(Ident(mod), List(Ident(param))), Apply(ident, Literal(Constant(term))::Nil), t1, t2))
        if name.decoded == "ValDef"
          && mod.decoded == "Modifiers"
          && param.decoded == "PARAM"
        =>
        Some(term.toString)
      case _ => None
    }

  }

}


trait SilkType[A]

case class SilkIntSeq(v:Seq[Int]) extends SilkType[Int] {
  def map[B](f: Int => B) : SilkMonad[B] = macro FunctionTree.mapImpl[Int, B]
}

case class SilkMonad[A](prev:SilkType[_], expr:String) extends SilkType[A] {

  def tree : ru.Tree = {
    val tb = scala.reflect.runtime.currentMirror.mkToolBox()
    tb.parse(expr).asInstanceOf[ru.Tree]
  }

  def map[B](f: A => B) : SilkMonad[B] = macro FunctionTree.mapImpl[A, B]
}

object FunctionTreeUtil {




}



