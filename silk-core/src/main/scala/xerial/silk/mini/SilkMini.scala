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
import java.io.{ObjectInputStream, ByteArrayInputStream, ByteArrayOutputStream, ObjectOutputStream}
import xerial.core.util.DataUnit
import xerial.silk.core.ClosureSerializer

object SilkContext {
  val defaultContext = new SilkContext

}

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

  def newSilk[A](in:Seq[A]) : SilkMini[A] = {
    val r = RawSeq(newID, this, in)
    r.setContext(this)
    r
  }
  def newSilkSingle[A](v:A) : SilkMini[A] = {
    val r = RawSeq(newID, this, Seq(v))
    r.setContext(this)
    r
  }

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


  def run[A](op:SilkMini[A]) {
    if(table.contains(op.id))
      return


    val buf = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(buf)
    oos.writeObject(op)
    oos.close()
    val ba = buf.toByteArray

    debug(s"submit: ${op}, byte size: ${DataUnit.toHumanReadableFormat(ba.length)}")

    scheduler.submit(Task(ba))
//
//    // TODO send the job to a remote machine
//    val result = op match {
//      case MapOp(sc, in, f, expr) =>
//        val splitResult = for(sp <- in.split) yield {
//          // Input.map(e => f(e))
//          evalRecursively(sp).map(e => evalSingleRecursively(f(e)))
//        }
//        splitResult.flatten
//      case FlatMapOp(sc, in, f, expr) =>
//        val splitResult = for(sp <- in.split) yield {
//          evalRecursively(sp).map(e => evalRecursively(f(e)))
//        }
//        splitResult.flatten
//      case RawSeq(sc, in) =>
//        in
//      case ReduceOp(sc, in, f, expr) =>
//        evalRecursively(in).reduce{evalSingleRecursively(f.asInstanceOf[(A,A)=>A](_, _))}
//      case _ =>
//        warn(s"unknown op: ${op}")
//        None
//    }
//
//    putIfAbsent(op.id, result)
  }


  private val scheduler = new Scheduler(this)
}

case class Task(opBinary:Array[Byte])

class Scheduler(sc:SilkContext) extends Logger {

  private val taskQueue = collection.mutable.Queue.empty[Task]

  def eval[A](op:SilkMini[A]) : Seq[A] = {
    sc.run(op)
    sc.get(op.id).asInstanceOf[Seq[A]]
  }

  def evalSingleRecursively[E](v:E) : E = {
    def loop(a:Any) : E = {
      a match {
        case s:SilkMini[_] => loop(eval(s))
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
        case s:SilkMini[_] =>  loop(eval(s))
        case s:Seq[_] => s.asInstanceOf[Seq[E]]
        case other =>
          throw Pending(s"invalid data type: ${other.getClass}")
      }
    }
    loop(v)
  }

  def submit(task:Task) = {
    taskQueue += task

    val ois = new ObjectInputStream(new ByteArrayInputStream(task.opBinary))
    val op = ois.readObject().asInstanceOf[SilkMini[_]]
    op.setContext(sc)
    debug(s"run: ${op}, sc:${op.getContext}, byte size: ${DataUnit.toHumanReadableFormat(task.opBinary.length)}")

    // TODO send the job to a remote machine
    val result = op match {
      case MapOp(in, f, expr) =>
        evalRecursively(in).map(e => evalSingleRecursively(f(e)))
      case FlatMapOp(in, f, expr) =>
        evalRecursively(in).flatMap{e =>
          debug(s"func:${f.getClass}")

          // TODO: dependency injection B$1 (variable) or method
          def setField(name:String, vv:SilkMini[_]) = {
            vv.setContext(sc)
            val fld = f.getClass.getDeclaredField(name)
            fld.setAccessible(true)
            fld.set(f, vv)
            debug(s"$name = $vv")
          }
          //setField("B$1", RawSeq(-1, sc, Seq(1)))
          setField("C$1", RawSeq(-2, sc, Seq(true)))
          val fv = f(e) // ! NPE
          evalRecursively(fv)
        }
      case RawSeq(id, sc, in) =>
        in
      case ReduceOp(in, f, expr) =>
        evalRecursively(in).reduce{evalSingleRecursively(f.asInstanceOf[(Any,Any)=>Any](_, _))}
      case _ =>
        warn(s"unknown op: ${op}")
        None
    }

    sc.putIfAbsent(op.id, result)

  }




}


object SilkMini {


  def newOp[F, Out](c:Context)(op:c.Tree, f:c.Expr[F]) = {
    import c.universe._
    val checked = c.typeCheck(f.tree)
    val t = c.reifyTree(c.universe.treeBuild.mkRuntimeUniverseRef, EmptyTree, checked)
    val exprGen = c.Expr[ru.Expr[F]](t).tree
    //val e = c.Expr[SilkMini[Out]](Apply(Select(op, newTermName("apply")), List(Select(c.prefix.tree, newTermName("sc")), c.prefix.tree, f.tree, exprGen)))
    val e = c.Expr[SilkMini[Out]](Apply(Select(op, newTermName("apply")), List(c.prefix.tree, f.tree, exprGen)))
    //val e = c.Expr[SilkMini[Out]](Apply(Select(op, newTermName("apply")), List(c.prefix.tree, Apply(Select(reify{ClosureSerializer}.tree, newTermName("cleanupF1")), List(f.tree)), exprGen)))

    reify {
      val silk = e.splice
      //silk.setContext(c.prefix.splice.asInstanceOf[SilkMini[_]].getContext)
      silk
    }
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
abstract class SilkMini[+A](val id:Int, @transient private var sc:SilkContext) extends Serializable with Logger {

  def setContext(newSC:SilkContext) = {this.sc = newSC}
  def getContext = sc

  def split: Seq[SilkMini[A]] = Seq(this)

  override def toString = {
    val cl = this.getClass
    val schema = ObjectSchema(cl)
    val params = for(p <- schema.constructor.params if p.name != "sc"; v = p.get(this) if v != null) yield {
      if(classOf[ru.Expr[_]].isAssignableFrom(p.valueType.rawType)) {
        s"${v}[${v.toString.hashCode}]"
      }
      else if(classOf[SilkMini[_]].isAssignableFrom(p.valueType.rawType)) {
        s"[${p.get(this).asInstanceOf[SilkMini[_]].id}]"
      }
      else
        v
    }

    val prefix = s"[$id]"
    val s = s"${cl.getSimpleName}(${params.mkString(", ")})"
    val fv = freeVariables
    val fvStr = if(fv == null || fv.isEmpty) "" else s"{${fv.mkString(", ")}}|= "
    val varDef = if(argVariable == null) "" else { argVariable.map(a => s"$a => ") getOrElse "" }
    s"${prefix}${fvStr}${varDef}$s"
  }

  def map[B](f: A=>B) : SilkMini[B] = macro mapImpl[A, B]
  def flatMap[B](f:A=>SilkMini[B]) : SilkMini[B] = macro flatMapImpl[A, B]

  def eval(sc:SilkContext): Seq[A] = {
    debug(s"eval: $this")
    require(sc != null, "sc must not be null")
    sc.run(this)
    sc.get(id).asInstanceOf[Seq[A]]
  }

  @transient val argVariable : Option[ValType] = None
  @transient val freeVariables : Set[ValType] = Set.empty

}


case class ValType(name:String, tpe:ru.Type) {
  override def toString = s"$name:${if(isSilkType) "*" else ""}$tpe"
  def isSilkType = {
    import ru._
    tpe <:< typeOf[SilkMini[_]]
  }
}


trait SplitOp[F, A] extends Logger { self: SilkMini[A] =>


  var fe:ru.Expr[F]

  @transient override val argVariable = {
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

  @transient override val freeVariables = {
    import ru._

    val fvNameSet = (for(v <- fe.tree.freeTerms) yield v.name.decoded).toSet
    debug(s"free variables: $fvNameSet")
    val b = Set.newBuilder[ValType]


    val tv = new Traverser {
      override def traverse(tree: ru.Tree) {
        def matchIdent(idt:Ident) : ru.Tree = {
          val name = idt.name.decoded
          if(fvNameSet.contains(name)) {
            val tt : ru.Tree = MacroUtil.toolbox.typeCheck(idt, silent=true)
            b += ValType(idt.name.decoded, tt.tpe)
            tt
          }
          else
            idt
        }

        tree match {
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

case class RawSeq[+A](override val id:Int, @transient c:SilkContext, in:Seq[A]) extends SilkMini[A](id, c)

case class MapOp[A, B](in:SilkMini[A], f:A=>B, @transient var fe:ru.Expr[A=>B]) extends SilkMini[B](in.getContext.newID, in.getContext) with SplitOp[A=>B, B]
case class FlatMapOp[A, B](in:SilkMini[A], f:A=>SilkMini[B], @transient var fe:ru.Expr[A=>SilkMini[B]]) extends SilkMini[B](in.getContext.newID, in.getContext) with SplitOp[A=>SilkMini[B], B] {
  debug(s"f class:${f.getClass}")
}
case class ReduceOp[A](in:SilkMini[A], f:(A, A) => A, @transient fe:ru.Expr[(A, A)=>A]) extends SilkMini[A](in.getContext.newID, in.getContext) with MergeOp


