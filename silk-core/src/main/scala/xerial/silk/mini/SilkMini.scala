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
import xerial.lens.{TypeUtil, ObjectSchema}
import xerial.core.log.Logger
import scala.collection.GenTraversableOnce
import xerial.silk.{MacroUtil, Pending, NotAvailable, SilkException}


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
  def newSilkSingle[A](v:A) : SilkMini[A] = RawSeq(this, Seq(v))

  def newID : Int =  {
    idCount += 1
    idCount
  }

  def get(id:Int) = {
    table(id)
  }

  def putIfAbsent[A](id:Int, v: => A) {
    if(!table.contains(id)) {
      table += id -> v
    }
  }


  def evalSingleRecursively[E](v:E) : E = {
    def loop(a:Any) : E = {
      a match {
        case s:SilkMini[_] => loop(s.eval)
        case other => other.asInstanceOf[E]
      }
    }
    loop(v)
  }

  /**
   * Execute and wait until the result becomes available
   * @param v
   * @tparam E
   * @return
   */
  def evalRecursively[E](v:SilkMini[E]) : Seq[E] = {
    def loop(a:Any) : Seq[E] = {
      a match {
        case s:SilkMini[_] => loop(s.eval)
        case s:Seq[_] => s.asInstanceOf[Seq[E]]
        case other =>
          throw Pending(s"invalid data type: ${other.getClass}")
      }
    }
    loop(v)
  }

  def run[A](op:SilkMini[A]) {
    if(table.contains(op.id))
      return

    debug(s"run: ${op}")
    // TODO send the job to a remote machine
    val result = op match {
      case MapOp(sc, in, f, expr) =>
        val splitResult = for(sp <- in.split) yield {
          // Input.map(e => f(e))
          evalRecursively(sp).map(e => evalSingleRecursively(f(e)))
        }
        splitResult.flatten
      case FlatMapOp(sc, in, f, expr) =>
        val splitResult = for(sp <- in.split) yield {
          evalRecursively(sp).map(e => evalRecursively(f(e)))
        }
        splitResult.flatten
      case RawSeq(sc, in) =>
        in
      case ReduceOp(sc, in, f, expr) =>
        evalRecursively(in).reduce{evalSingleRecursively(f.asInstanceOf[(A,A)=>A](_, _))}
      case _ =>
        warn(s"unknown op: ${op}")
        None
    }

    putIfAbsent(op.id, result)
  }


  private val scheduler = new Scheduler
}

case class Task()

class Scheduler {

  private val taskQueue = collection.mutable.Queue.empty[Task]

  def submit(task:Task) = {
    taskQueue += task
  }




}


object SilkMini {

  def newOp[F, Out](c:Context)(op:c.Tree, f:c.Expr[F]) = {
    import c.universe._
    val checked = c.typeCheck(f.tree)
    val t = c.reifyTree(c.universe.treeBuild.mkRuntimeUniverseRef, EmptyTree, checked)
    val exprGen = c.Expr[ru.Expr[F]](t).tree
    c.Expr[SilkMini[Out]](Apply(Select(op, newTermName("apply")), List(Select(c.prefix.tree, newTermName("sc")), c.prefix.tree, f.tree, exprGen)))
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

  def split: Seq[SilkMini[A]] = Seq(this)

  override def toString = {
    val cl = this.getClass
    val schema = ObjectSchema(cl)
    val params = for(p <- schema.constructor.params if p.name != "sc") yield {
      if(classOf[ru.Expr[_]].isAssignableFrom(p.valueType.rawType)) {
        val v = p.get(this)
        s"${v}[${v.toString.hashCode}]"
      }
      else if(classOf[SilkMini[_]].isAssignableFrom(p.valueType.rawType)) {
        s"[${p.get(this).asInstanceOf[SilkMini[_]].id}]"
      }
      else
        p.get(this)
    }

    val prefix = s"[$id]"
    val s = s"${cl.getSimpleName}(${params.mkString(", ")})"
    val fv = freeVariables
    val fvStr = if(fv.isEmpty) "" else s"{${fv.mkString(", ")}}|= "
    val varDef = argVariable.map(a => s"$a => ") getOrElse ""
    s"${prefix}${fvStr}${varDef}$s"
  }

  def map[B](f: A=>B) : SilkMini[B] = macro mapImpl[A, B]
  def flatMap[B](f:A=>SilkMini[B]) : SilkMini[B] = macro flatMapImpl[A, B]

  /**
   * Execute and wait until the result is available
   * @return
   */
  def eval: Seq[A] = {
    sc.run(this)
    sc.get(id).asInstanceOf[Seq[A]]
  }

  def argVariable : Option[ValType] = None
  def freeVariables : Set[ValType] = Set.empty

}


case class ValType(name:String, tpe:ru.Type) {
  override def toString = s"$name:${if(isSilkType) "*" else ""}$tpe"
  def isSilkType = {
    import ru._
    tpe <:< typeOf[SilkMini[_]]
  }
}


trait SplitOp[F, A] extends Logger { self: SilkMini[A] =>

  val fe:ru.Expr[F]

  override def argVariable = {
    import ru._
    fe.tree match {
      case f @ Function(List(ValDef(mod, name, e1, e2)), body) =>
        fe.staticType match {
          case TypeRef(prefix, symbol, List(from, to)) =>
            Some(ValType(name.decoded, from))
          case _ => None
        }
      case _ => None
    }
  }

  override def freeVariables = {
    import ru._
    val tb = MacroUtil.toolbox

    val fvNameSet = (for(v <- fe.tree.freeTerms) yield v.name.decoded).toSet
    val b = Set.newBuilder[ValType]

    val tv = new Traverser {
      override def traverse(tree: ru.Tree) {
        def matchIdent(idt:Ident) : ru.Tree = {
          val name = idt.name.decoded
          if(fvNameSet.contains(name)) {
            val tt : ru.Tree = tb.typeCheck(idt, silent=true)
            b += ValType(idt.name.decoded, tt.tpe)
            tt
          }
          else
            idt
        }

        tree match {
//          case Apply(sl @ Select(idt @ Ident(term), method), args) =>
//            val tt = matchIdent(idt)
//            if(!(tt.tpe != null && tt.tpe <:< typeOf[SilkMini[_]])) {
//              traverse(sl)
//              args.foreach(traverse)
//            }
          case idt @ Ident(term) =>
            matchIdent(idt)
          case other => super.traverse(other)
        }
      }
    }
    //debug(showRaw(fe.tree))
    tv.traverse(fe.tree)

    b.result
  }

}
trait MergeOp

case class RawSeq[+A](override val sc:SilkContext, in:Seq[A]) extends SilkMini[A](sc) {
  //override def split = (for(s <- in.sliding(2, 2)) yield RawSeq(sc, s)).toIndexedSeq
}

case class MapOp[A, B](override val sc:SilkContext, in:SilkMini[A], f:A=>B, fe:ru.Expr[A=>B]) extends SilkMini[B](sc) with SplitOp[A=>B, B]
case class FlatMapOp[A, B](override val sc:SilkContext, in:SilkMini[A], f:A=>SilkMini[B], fe:ru.Expr[A=>SilkMini[B]]) extends SilkMini[B](sc) with SplitOp[A=>SilkMini[B], B]
case class ReduceOp[A](override val sc:SilkContext, in:SilkMini[A], f:(A, A) => A, fe:ru.Expr[(A, A)=>A]) extends SilkMini[A](sc) with MergeOp


