//--------------------------------------
//
// SilkMini.scala
// Since: 2013/05/17 12:28 PM
//
//--------------------------------------

package xerial.silk.mini
import scala.reflect.runtime.{universe=>ru}
import scala.language.experimental.macros
import scala.reflect.macros.Context
import xerial.lens.ObjectSchema
import xerial.core.log.Logger



class SilkContext() extends Logger {
  private var idCount = 0
  private val table = collection.mutable.Map[Int, Any]()

  override def toString = {
    val b = new StringBuilder
    b append "[values]\n"
    for(id <- table.keys.toSeq.sorted) {
      b append s" [$id]: ${table(id)}"
      b append "\n"
    }
    b.result.trim
  }

  def newSilk[A](in:Seq[A]) : SilkMini[A] = RawSeq(this, in)

  def newID : Int =  {
    idCount += 1
    idCount
  }

  def get(id:Int) = table(id)

  def putIfAbsent[A](id:Int, v: => A) {
    if(!table.contains(id)) {
      trace(s"sc.put($id):$v")
      table += id -> v
    }
  }


  def evalSingleRecursively[E](v:E) : E = {
    def loop(a:Any) : E = {
      a match {
        case s:SilkMini[_] => loop(s.eval.head)
        case other => other.asInstanceOf[E]
      }
    }
    loop(v)
  }
  def evalRecursively[E](v:SilkMini[E]) : Seq[E] = {
    def loop(a:Any) : Seq[E] = {
      a match {
        case s:SilkMini[_] => loop(s.eval)
        case other => other.asInstanceOf[Seq[E]]
      }
    }
    loop(v)
  }

  def run[A](op:SilkMini[A]) {
    if(table.contains(op.id))
      return

    debug(s"run: ${op.id}")
    // TODO send the job to a remote machine
    val result = op match {
      case MapOp(sc, in, f, expr) =>
        in.eval.map(e => evalSingleRecursively(f(e)))
      case FlatMapOp(sc, in, f, expr) =>
        in.eval.flatMap(e => evalRecursively(f(e)))
      case RawSeq(sc, in) =>
        in
      case ReduceOp(sc, in, f, expr) =>
        in.eval.reduce{evalSingleRecursively(f.asInstanceOf[(A,A)=>A](_, _))}
      case _ =>
        warn(s"unknown op: ${op}")
        None
    }

    putIfAbsent(op.id, result)
  }

}


object SilkMini {

  def newOp[F, Out](c:Context)(op:c.Tree, f:c.Expr[F]) = {
    import c.universe._
    val checked = c.typeCheck(f.tree)
    val t = c.reifyTree(c.universe.treeBuild.mkRuntimeUniverseRef, EmptyTree, checked)
    val exprGen = c.Expr[ru.Expr[F]](t).tree
    c.Expr[SilkMini[Out]]( Apply(Select(op, newTermName("apply")), List(Select(c.prefix.tree, newTermName("sc")), c.prefix.tree, f.tree, exprGen)))
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
 * Mini-implementation of the Silk framework
 */
abstract class SilkMini[+A](val sc:SilkContext) {
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

  def eval: Seq[A] = {
    sc.run(this)
    sc.get(id).asInstanceOf[Seq[A]]
  }

}


case class RawSeq[A](override val sc:SilkContext, in:Seq[A]) extends SilkMini[A](sc){

}

case class MapOp[A, B](override val sc:SilkContext, in:SilkMini[A], f:A=>B, fe:ru.Expr[A=>B]) extends SilkMini[B](sc){

}
case class FlatMapOp[A, B](override val sc:SilkContext, in:SilkMini[A], f:A=>SilkMini[B], fe:ru.Expr[A=>SilkMini[B]]) extends SilkMini[B](sc) {

}

case class ReduceOp[A](override val sc:SilkContext, in:SilkMini[A], f:(A, A) => A, fe:ru.Expr[(A, A)=>A]) extends SilkMini[A](sc) {

}

