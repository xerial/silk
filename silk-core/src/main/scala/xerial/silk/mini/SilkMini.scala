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
import xerial.lens.ObjectSchema


class SilkContext() {
  private var idCount = 0
  private val table = collection.mutable.Map[Int, Any]()

  override def toString = s"SilkContext"

  def newSilk[A](in:Seq[A]) : SilkMini[A] = RawSeq(this, in)

  def newID : Int =  {
    idCount += 1
    idCount
  }

  def get(id:Int) = table(id)

  def put[A](id:Int, v:A) {
    table += id -> v
  }

}


object SilkMini {

  def newOp[F, Out](c:Context)(op:c.Tree, f:c.Expr[F]) = {
    import c.universe._
    val t = c.reifyTree(c.universe.treeBuild.mkRuntimeUniverseRef, EmptyTree, c.typeCheck(f.tree))
    val exprGen = c.Expr[Expr[ru.Expr[F]]](t)
    c.Expr[SilkMini[Out]]( Apply(Select(op, newTermName("apply")), List(Select(c.prefix.tree, newTermName("sc")), c.prefix.tree, f.tree, exprGen.tree)))
  }

  def mapImpl[A, B](c:Context)(f:c.Expr[A=>B]) = {
    newOp[A=>B, B](c)(c.universe.reify{MapOp}.tree, f)
  }

  def flatMapImpl[A, B](c:Context)(f:c.Expr[A=>SilkMini[B]]) = {
    newOp[A=>SilkMini[B], B](c)(c.universe.reify{FlatMapOp}.tree, f)
  }

}

import SilkMini._

/**
 * Mini-implementation of the framework
 */
abstract class SilkMini[A](val sc:SilkContext) {
  val id = sc.newID

  override def toString = {
    val cl = this.getClass
    val schema = ObjectSchema(cl)
    val params = for(p <- schema.constructor.params if p.name != "sc") yield {
      p.get(this)
    }
    s"[$id]:${cl.getSimpleName}(${params.mkString(", ")}})"
  }


  def map[B](f: A=>B) : SilkMini[B] = macro mapImpl[A, B]
  def flatMap[B](f:A=>SilkMini[B]) : SilkMini[B] = macro flatMapImpl[A, B]

  def eval: Seq[A]
}

case class RawSeq[A](override val sc:SilkContext, in:Seq[A]) extends SilkMini[A](sc){
  def eval = {
    sc.put(id, in)
    in
  }
}

case class MapOp[A, B](override val sc:SilkContext, in:SilkMini[A], f:A=>B, fe:ru.Expr[A=>B]) extends SilkMini[B](sc){
  def eval = {
    val result = for(e <- in.eval) yield {
      val r = f(e) match {
        case s:SilkMini[_] => s.eval.head
        case other => other
      }
      r.asInstanceOf[B]
    }
    sc.put(id, result)
    result
  }
}
case class FlatMapOp[A, B](override val sc:SilkContext, in:SilkMini[A], f:A=>SilkMini[B], fe:ru.Expr[A=>SilkMini[B]]) extends SilkMini[B](sc) {
  def eval = {
    val result = in.eval.flatMap{ e =>
      val r = f(e).eval match {
        case s:SilkMini[_] => s.eval
        case other => other
      }
      r.asInstanceOf[Seq[B]]
    }
    sc.put(id, result)
    result
  }
}

