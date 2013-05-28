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
import xerial.silk.{MacroUtil, Pending, NotAvailable, SilkException}
import java.io.{ObjectInputStream, ByteArrayInputStream, ByteArrayOutputStream, ObjectOutputStream}
import xerial.core.util.DataUnit
import scala.language.existentials

object SilkContext {
  val defaultContext = new SilkContext

  /**
   * Generating a new RawSeq instance of SilkMini[A] and register the input data to
   * the value holder of SilkContext. To avoid double registration, this method retrieves
   * enclosing method name and use it as a key for the cache table.
   * @param c
   * @param in
   * @tparam A
   * @return
   */
  def newSilkImpl[A](c:Context)(in:c.Expr[Seq[A]]) : c.Expr[SilkMini[A]] = {
    import c.universe._
    val m = c.enclosingMethod
    val methodName = m match {
      case DefDef(mod, name, _, _, _, _) =>
        name.decoded
      case _ => "unknown"
    }
    val mne = c.Expr[String](Literal(Constant(methodName)))
    val self = c.Expr[Class[_]](This(tpnme.EMPTY))
    reify{
      val sc = c.prefix.splice.asInstanceOf[SilkContext]
      val input = in.splice
      val fref = FRef(self.splice.getClass, mne.splice)
      val id = sc.seen.getOrElseUpdate(fref, sc.newID)
      val r = RawSeq(fref, id, input)
      r.setContext(sc)
      sc.putIfAbsent(id, input)
      r
    }
  }

}

class SilkContext() extends Logger {
  private var idCount = 0
  private[silk] val seen = collection.mutable.Map[FRef[_], Int]()
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


  def newSilk[A](in:Seq[A]) : SilkMini[A] = macro SilkContext.newSilkImpl[A]



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

    info(s"submit: ${op}, byte size: ${DataUnit.toHumanReadableFormat(ba.length)}")

    scheduler.submit(Task(ba))
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
        case s:SilkMini[_] =>
          loop(eval(s))
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
    debug(s"run: ${op}, byte size: ${DataUnit.toHumanReadableFormat(task.opBinary.length)}")

    // TODO send the job to a remote machine
    val result = op match {
      case MapOp(in, f, expr) =>
        evalRecursively(in).map{e => evalSingleRecursively(f(e))}
      case FlatMapOp(in, f, expr) =>
        evalRecursively(in).flatMap{e => evalRecursively(f(e))}
      case RawSeq(fref, id, in) =>
        in
//      case ReduceOp(in, f, expr) =>
//        evalRecursively(in).reduce{evalSingleRecursively(f.asInstanceOf[(Any,Any)=>Any](_, _))}
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

    /**
     * Resolve prefix
     */
//    val fref = c.prefix.tree match {
//      case Select(This(typeName), term) =>
//        FRef(typeName.decoded, term.decoded)
//      case _ =>
//        println(s"unknown prefix: ${showRaw(c.prefix.tree)}")
//        FRef("unknown", "term")
//    }
//    val frefTree = reify{FRef(c.Expr[String](Literal(Constant(fref.owner))).splice, c.Expr[String](Literal(Constant(fref.name))).splice)}.tree

    /**
     * Removes nested reifyTree application to Silk operations.
     */
    object RemoveDoubleReify extends Transformer {
      override def transform(tree: c.Tree) = {
        tree match {
          case Apply(TypeApply(s @ Select(idt @ Ident(q), termname), _), reified::tail )
             if termname.decoded == "apply" && q.decoded.endsWith("Op")
          => c.unreifyTree(reified)
          case _ => super.transform(tree)
        }
      }
    }
    val rmdup = RemoveDoubleReify.transform(f.tree)
    val checked = c.typeCheck(rmdup)

    val t = c.reifyTree(c.universe.treeBuild.mkRuntimeUniverseRef, EmptyTree, checked)
    val exprGen = c.Expr[ru.Expr[F]](t).tree
    val e = c.Expr[SilkMini[Out]](Apply(Select(op, newTermName("apply")), List(c.prefix.tree, f.tree, exprGen)))
    reify {
      val silk = e.splice
      silk
    }
  }

//
//
//
//  def rawSeqImpl[A](c:Context) = {
//    import c.universe._
//    val str = c.prefix.toString()
//    val e = c.Expr[String](Literal(Constant(str)))
//    reify {
//      val r = RawSeq(null, -1, c.prefix.splice.asInstanceOf[SilkFactory.ToSilk[_]].in)
//      r
//    }
//  }

  def mapImpl[A, B](c:Context)(f:c.Expr[A=>B]) = {
    newOp[A=>B, B](c)(c.universe.reify{MapOp}.tree, f)
  }

  def flatMapImpl[A, B](c:Context)(f:c.Expr[A=>SilkMini[B]]) = {
    newOp[A=>SilkMini[B], B](c)(c.universe.reify{FlatMapOp}.tree, f)
  }



}

import SilkMini._

trait SilkM

/**
 * Mini-implementation of the Silk framework
 */
abstract class SilkMini[+A](@transient var sc:SilkContext, val id:Int) extends Serializable with SilkM with Logger {

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

  //private[silk] def newSilk[B](sc:SilkContext, id:Int, in:Seq[B]) : SilkMini[B] = macro rawSeqImpl[B]

  def map[B](f: A=>B) : SilkMini[B] = macro mapImpl[A, B]
  def flatMap[B](f:A=>SilkMini[B]) : SilkMini[B] = macro flatMapImpl[A, B]

  def eval: Seq[A] = {
    debug(s"eval: $this")
    require(sc != null, "sc must not be null")
    sc.run(this)
    sc.get(id).asInstanceOf[Seq[A]]
  }

  @transient val argVariable : Option[ValType] = None
  @transient val freeVariables : Seq[ValType] = Seq.empty

}


object SilkFactory {



}

case class ValType(name:String, tpe:ru.Type) {
  override def toString = s"$name:${if(isSilkType) "*" else ""}$tpe"
  def isSilkType = {
    import ru._
    tpe <:< typeOf[SilkMini[_]]
  }
}

/**
 * Function reference
 */

case class FRef[A](owner:Class[A], name:String)


case class RawSeq[+A](fref:FRef[_], override val id:Int, @transient in:Seq[A]) extends SilkMini[A](null, id)

case class MapOp[A, B](in:SilkMini[A], f:A=>B, @transient var fe:ru.Expr[A=>B]) extends SilkMini[B](in.getContext, in.getContext.newID) with SplitOp[A=>B, B]
case class FlatMapOp[A, B](in:SilkMini[A], f:A=>SilkMini[B], @transient var fe:ru.Expr[A=>SilkMini[B]]) extends SilkMini[B](in.getContext, in.getContext.newID) with SplitOp[A=>SilkMini[B], B] {
  trace(s"f class:${f.getClass}")
}
//case class ReduceOp[A](in:SilkMini[A], f:(A, A) => A, @transient fe:ru.Expr[(A, A)=>A]) extends SilkMini[A](in.getContext.newID, in.getContext) with MergeOp


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
    val b = Seq.newBuilder[ValType]

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
    tv.traverse(fe.tree)

    // Remove duplicate occurrences.
    b.result.distinct
  }

}


trait MergeOp
