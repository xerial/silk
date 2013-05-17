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
      debug(s"sc.put($id):$v")
      table += id -> v
    }
  }


  def run[A, B, O](in:SilkMini[A], body:SilkMini[B], f:Seq[A] => O) {

    if(!table.contains(in.id)) {
      in.eval
    }
    debug(s"run ${body}")
    // TODO send job to a remote machine
    val result = f(get(in.id).asInstanceOf[Seq[A]])
    putIfAbsent(body.id, result)
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

  def eval: Seq[A]

  protected def evalSingleFully[E](v:E) : E = {
    def loop(a:Any) : E = {
      a match {
        case s:SilkMini[_] => loop(s.eval.head)
        case other => other.asInstanceOf[E]
      }
    }
    loop(v)
  }

  protected def evalFully[E](v:SilkMini[E]) : Seq[E] = {
    def loop(a:Any) : Seq[E] = {
      a match {
        case s:SilkMini[_] => loop(s.eval)
        case other => other.asInstanceOf[Seq[E]]
      }
    }
    loop(v)
  }
}


case class RawSeq[A](override val sc:SilkContext, in:Seq[A]) extends SilkMini[A](sc){
  def eval = {
    sc.putIfAbsent(id, in)
    in
  }
}

case class MapOp[A, B](override val sc:SilkContext, in:SilkMini[A], f:A=>B, fe:ru.Expr[A=>B]) extends SilkMini[B](sc){
  def eval = {
    sc.run(in, this, { input : Seq[A] => input.map(e => evalSingleFully[B](f(e))) })
    sc.get(id).asInstanceOf[Seq[B]]
  }
}
case class FlatMapOp[A, B](override val sc:SilkContext, in:SilkMini[A], f:A=>SilkMini[B], fe:ru.Expr[A=>SilkMini[B]]) extends SilkMini[B](sc) {
  def eval = {
    sc.run(in, this, { input : Seq[A] => input.flatMap(e => evalFully(f(e))) })
    sc.get(id).asInstanceOf[Seq[B]]
  }
}

case class ReduceOp[A](override val sc:SilkContext, in:SilkMini[A], f:(A, A) => A, fe:ru.Expr[(A, A)=>A]) extends SilkMini[A](sc) {
  def eval = {
    sc.run(in, this, { input : Seq[A] => Seq(input.reduce{(prev:A, next:A) => evalSingleFully(f(prev, next))})})
    sc.get(id).asInstanceOf[Seq[A]]
  }

}

