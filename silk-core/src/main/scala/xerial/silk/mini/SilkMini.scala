//--------------------------------------
//
// SilkMini.scala
// Since: 2013/05/17 12:28 PM
//
//--------------------------------------
/**
 * Mini implementation of the silk framework
 */
package xerial.silk.mini
import scala.reflect.runtime.{universe=>ru}
import scala.language.experimental.macros
import scala.reflect.macros.Context


class SilkContext() {
  private var idCount = 0

  override def toString = s"SilkContext"

  def newSilk[A](in:Seq[A]) : SilkMini[A] = RawSeq(this, in)

  def newID : Int =  {
    idCount += 1
    idCount
  }
}


object SilkMini {

  def mapImpl[A, B](c:Context)(f:c.Expr[A=>B]) = {
    import c.universe._
    val t = c.reifyTree(c.universe.treeBuild.mkRuntimeUniverseRef, EmptyTree, c.typeCheck(f.tree))
    val exprGen = c.Expr[Expr[ru.Expr[A=>B]]](t)
    c.Expr[SilkMini[B]]( Apply(Select(reify{MapOp}.tree, newTermName("apply")), List(Select(c.prefix.tree, newTermName("sc")), c.prefix.tree, f.tree, exprGen.tree)))
  }

  def flatMapImpl[A, B](c:Context)(f:c.Expr[A=>SilkMini[B]]) = {
    import c.universe._
    val t = c.reifyTree(c.universe.treeBuild.mkRuntimeUniverseRef, EmptyTree, c.typeCheck(f.tree))
    val exprGen = c.Expr[Expr[ru.Expr[A=>SilkMini[B]]]](t)
    c.Expr[SilkMini[B]]( Apply(Select(reify{FlatMapOp}.tree, newTermName("apply")), List(Select(c.prefix.tree, newTermName("sc")), c.prefix.tree, f.tree, exprGen.tree)))
  }

}

import SilkMini._

/**
 * Mini-implementation of the framework
 */
abstract class SilkMini[A](val sc:SilkContext) {
  val id = sc.newID

  def map[B](f: A=>B) = macro mapImpl[A, B]
  def flatMap[B](f:A=>SilkMini[B]) = macro flatMapImpl[A, B]

}

case class RawSeq[A](override val sc:SilkContext, in:Seq[A]) extends SilkMini[A](sc)
case class MapOp[A, B](override val sc:SilkContext, in:SilkMini[A], f:A=>B, fe:ru.Expr[A=>B]) extends SilkMini[B](sc)
case class FlatMapOp[A, B](override val sc:SilkContext, in:SilkMini[A], f:A=>SilkMini[B], fe:ru.Expr[A=>SilkMini[B]]) extends SilkMini[B](sc)

