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


/**
 * @author Taro L. Saito
 */
object FunctionTree extends Logger {

  def newSilkMonad[A, B](f:ru.Expr[A=>B]) : SilkMonad[B] = new SilkMonad[B](f)



  def mapImpl[A:c.WeakTypeTag, B:c.WeakTypeTag](c:Context)(f:c.Expr[A=>B]) = {
    import c.universe._

    object SingleReification extends Transformer {
      def apply(tree:Tree) = transform(tree)
      override def transform(tree:Tree) : Tree = {
        tree match {
          case Apply(Select(q, termname), reified::tail) if termname.toString == "map" =>
            c.unreifyTree(reified)
          case Apply(TypeApply(Select(q, termname), _), reified::tail) if termname.toString == "map" =>
            c.unreifyTree(reified)
          case _ =>
            tree
        }
      }
    }

    println(f.tree)

    val fTree = Apply(Select(c.prefix.tree, newTermName("map")), List(f.tree))
    val mapTree = SingleReification(fTree)
    val checked = c.typeCheck(mapTree)

    val t = c.Expr[ru.Expr[A=>B]](c.reifyTree(treeBuild.mkRuntimeUniverseRef, EmptyTree, checked))
    println(t.tree)
    ru.reify{ newSilkMonad[A, B]( f ) }
  }
}


class SilkIntSeq(v:Seq[Int]) {
  def map[B](f: Int => B) = macro FunctionTree.mapImpl[Int, B]
}

class SilkMonad[A](tree:ru.Expr[_]) {
  def map[B](f: A => B) = macro FunctionTree.mapImpl[A, B]
}
