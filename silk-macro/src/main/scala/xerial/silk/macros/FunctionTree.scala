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

    // Create an AST for generating runtime Expr[A=>B]
    val t = c.reifyTree(c.universe.treeBuild.mkRuntimeUniverseRef, EmptyTree, c.typeCheck(f.tree))
    val exprGeneratorCode = c.Expr[Expr[ru.Expr[A=>B]]](t)
    c.Expr[SilkMonad[B]](Apply(Select(reify{SilkMonad}.tree, newTermName("apply")), List(c.prefix.tree, exprGeneratorCode.tree)))
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
      case Apply(c, List(arg)) =>
        c match {
          case s@Select(Ident(c1), m2)
            =>
            Some(MethodCall(IdentRef(c1.decoded), m2.toString, arg))
          case _ => None
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
      // unary function
      case Function(List(BindToVal(valBind)), body) => Some(FunCall(valBind, body))
      case _ => None
    }
  }
}



object BindToVal {

  import ru._

  def unapply(t:Tree) : Option[String] = {
    t match {
      case ValDef(mod, term, t1, t2) => Some(term.toString)
      case _ => None
    }

  }

}


trait SilkType[A]

case class SilkIntSeq(v:Seq[Int]) extends SilkType[Int] {
  def map[B](f: Int => B) : SilkMonad[B] = macro FunctionTree.mapImpl[Int, B]
}

case class SilkMonad[A](prev:SilkType[_], expr:ru.Expr[_]) extends SilkType[A] with Logger {

  def tree : ru.Tree = expr.tree

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

