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

  def collectMethodCall(t:ru.Tree) : Seq[MethodCall] = {
    import ru._

    val b = Seq.newBuilder[MethodCall]

    object traverser extends Traverser {
      override def traverse(tree: ru.Tree) {
        tree match {
          case MethodCall(fcall) =>
            b += fcall
            //super.traverse(tree)
          case _ => super.traverse(tree)
        }
      }
    }

    traverser.traverse(t)
    b.result
  }


}


trait MethodOwnerRef
case class ThisTypeRef(tpeName:String) extends MethodOwnerRef {
  override def toString = s"this"
}
case class ClassTypeRef(tpeName:String) extends MethodOwnerRef
case class PackageRef(fullPath:String) extends MethodOwnerRef
case class IdentRef(name:String) extends MethodOwnerRef


case class MethodCall(cls:MethodOwnerRef, methodName:String, body:ru.Tree)
case class FunCall(valName:String, body:ru.Tree)

object MethodCall extends Logger {
  import ru._

  def unapply(t:Tree) : Option[MethodCall] = {
    t match {
      case Apply(Id("Apply"),
            List(Apply(Id("Select"),
              List(cls,
               Apply(Id("newTermName"),
                List(Literal(Constant(method)))))), tail))
      =>
        cls match {
          case ThisTypeRef(ttr) => Some(MethodCall(ttr, method.toString, tail))
          case Apply(Id("Ident"), List(Apply(Id("newTermName"), List(Literal(Constant(name))))))
          => Some(MethodCall(IdentRef(name.toString), method.toString, tail))
          case Apply(Id("Ident"), List(PackageRef(pkg)))
          => Some(MethodCall(pkg, method.toString, tail))
          case _ =>
            warn(s"unknown cls type: ${showRaw(cls)}")
            None
        }
      case _ => None
    }
  }
}

object Id {
  import ru._
  def unapply(t:ru.Tree) : Option[String] = {
    t match {
      case Ident(name) => Some(name.decoded)
      case _ => None
    }
  }
}

object PackageRef extends Logger {
  import ru._
  def unapply(t:ru.Tree) : Option[PackageRef] = {
    info(showRaw(t))
    def loop(tree:ru.Tree, tail:List[String]) : List[String] = {
      tree match {
        case Select(qual, name) =>
          loop(qual, name.toString::tail)
        case Id(name) => name::tail
        case _ => tail
      }
    }
    val res = loop(t, Nil)
    if(res.isEmpty)
      None
    else
      Some(PackageRef(res.mkString(".")))
  }

}

object ThisTypeRef {
  import ru._
  def unapply(t:ru.Tree) : Option[ThisTypeRef] = {
    t match {
      case Apply(Id("This"), List(Apply(Id("newTypeName"), List(Literal(Constant(tpName))))))
      => Some(ThisTypeRef(tpName.toString))
      case _ => None
    }
  }
}


object FunCall {
  import ru._

  def unapply(t:Tree) : Option[FunCall] = {
    t match {
      case Apply(Id("Function"), List(Apply(h, List(BindToVal(valBind))), body))
      => Some(FunCall(valBind.toString, body))
      // Function call
      case _ => None
    }
  }
}



object BindToVal {

  import ru._

  def unapply(t:Tree) : Option[String] = {
    t match {
      case Apply(Id("ValDef"), List(Apply(mode, param), Apply(ident, Literal(Constant(term))::Nil), t1, t2)) =>
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

  def functionCall : FunCall = {
    val lst = tree collect {
      case FunCall(fcall) => fcall
    }
    if(lst.isEmpty)
      sys.error("no function call is found")
    else if(lst.size > 1)
      sys.error("more than one function call is found")
    else
      lst.head
  }


  def map[B](f: A => B) : SilkMonad[B] = macro FunctionTree.mapImpl[A, B]
}

object FunctionTreeUtil {




}



