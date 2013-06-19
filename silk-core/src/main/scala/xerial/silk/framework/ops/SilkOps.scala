//--------------------------------------
//
// SilkOps.scala
// Since: 2013/06/16 16:13
//
//--------------------------------------

package xerial.silk.framework.ops

import scala.language.existentials
import scala.language.experimental.macros
import scala.reflect.ClassTag
import xerial.lens.{Parameter, ObjectSchema}
import xerial.core.log.Logger
import java.util.UUID
import scala.reflect.runtime.{universe=>ru}
import java.io._
import scala.reflect.macros.Context
import xerial.silk.util.MacroUtil

object SilkOps {

  def newUUID: UUID = UUID.randomUUID

  def newUUIDOf[A](in: Seq[A]): UUID = {
    val b = new ByteArrayOutputStream
    val os = new ObjectOutputStream(b)
    for (x <- in)
      os.writeObject(x)
    os.close
    UUID.nameUUIDFromBytes(b.toByteArray)
  }





  class MacroHelper[C <: Context](val c: C) {

    import c.universe._

    /**
     * Removes nested reifyTree application to Silk operations.
     */
    object RemoveDoubleReify extends Transformer {
      override def transform(tree: c.Tree) = {
        tree match {
          case Apply(TypeApply(s@Select(idt@Ident(q), termname), _), reified :: tail)
            if termname.decoded == "apply" && q.decoded.endsWith("Op")
          => c.unreifyTree(reified)
          case _ => super.transform(tree)
        }
      }
    }

    def removeDoubleReify(tree: c.Tree) = {
      RemoveDoubleReify.transform(tree)
    }

    def createFContext: c.Expr[FContext[_]] = {
      val m = c.enclosingMethod
      val methodName = m match {
        case DefDef(mod, name, _, _, _, _) =>
          name.decoded
        case _ => "<init>"
      }

      val mne = c.literal(methodName)
      val self = c.Expr[Class[_]](This(tpnme.EMPTY))
      val vd = findValDef
      val vdTree = vd.map {
        v =>
          val nme = c.literal(v.name.decoded)
          reify {
            Some(nme.splice)
          }
      } getOrElse {
        reify {
          None
        }
      }
      //println(s"vd: ${showRaw(vdTree)}")
      reify {
        FContext(self.splice.getClass, mne.splice, vdTree.splice)
      }
    }

    // Find a target variable of the operation result by scanning closest ValDefs
    def findValDef: Option[ValOrDefDef] = {

      def print(p: c.Position) = s"${p.line}(${p.column})"

      val prefixPos = c.prefix.tree.pos

      class Finder extends Traverser {

        var enclosingDef: List[ValOrDefDef] = List.empty
        var cursor: c.Position = null

        private def contains(p: c.Position, start: c.Position, end: c.Position) =
          start.precedes(p) && p.precedes(end)

        override def traverse(tree: Tree) {
          if (tree.pos.isDefined)
            cursor = tree.pos
          tree match {
            // Check whether the rhs of variable definition contains the prefix expression
            case vd@ValDef(mod, varName, tpt, rhs) =>
              // Record the start and end positions of the variable definition block
              val startPos = vd.pos
              super.traverse(rhs)
              val endPos = cursor
              //println(s"val $varName range:${print(startPos)} - ${print(endPos)}: prefix: ${print(prefixPos)}")
              if (contains(prefixPos, startPos, endPos)) {
                enclosingDef = vd :: enclosingDef
              }
            case other =>
              super.traverse(other)
          }
        }

        def enclosingValDef = enclosingDef.reverse.headOption
      }

      val f = new Finder()
      val m = c.enclosingMethod
      if(m == null)
        f.traverse(c.enclosingClass)
      else
        f.traverse(m)
      f.enclosingValDef
    }

  }


  /**
   * Generating a new RawSeq instance of SilkMini[A] and register the input data to
   * the value holder of SilkSession. To avoid double registration, this method retrieves
   * enclosing method name and use it as a key for the cache table.
   * @param c
   * @param in
   * @tparam A
   * @return
   */
  def newSilkImpl[A](c: Context)(in: c.Expr[Seq[A]])(ev: c.Expr[ClassTag[A]]): c.Expr[SilkOps[A]] = {
    import c.universe._

    val helper = new MacroHelper[c.type](c)
    //println(s"newSilk(in): ${in.tree.toString}")
    val frefExpr = helper.createFContext
    reify {
      val input = in.splice
      val fref = frefExpr.splice
      //val id = ss.seen.getOrElseUpdate(fref, ss.newID)
      val r = RawSeq(fref, input)(ev.splice)
      //SilkOps.cache.putIfAbsent(r.uuid, Seq(RawSlice(Host("localhost", "127.0.0.1"), 0, input)))
      r
    }
  }

  def loadImpl[A](c:Context)(file:c.Expr[File])(ev:c.Expr[ClassTag[A]]) = {
    import c.universe._
    val helper = new MacroHelper[c.type](c)
    //println(s"newSilk(in): ${in.tree.toString}")
    val frefExpr = helper.createFContext
    reify {
      val fref = frefExpr.splice
      val r = LoadFile[A](fref, file.splice)(ev.splice)
      r
    }
  }


  def newOp[F, Out](c: Context)(op: c.Tree, f: c.Expr[F]) = {
    import c.universe._

    val helper = new MacroHelper[c.type](c)
    val ft = f.tree // c.resetLocalAttrs(f.tree)
    val rmdup = helper.removeDoubleReify(ft)
    val checked = c.typeCheck(rmdup)
    val t = c.reifyTree(c.universe.treeBuild.mkRuntimeUniverseRef, EmptyTree, checked)
    val exprGen = c.Expr[ru.Expr[F]](t).tree
    val frefTree = helper.createFContext.tree.asInstanceOf[c.Tree]
    val e = c.Expr[SilkOps[Out]](Apply(Select(op, newTermName("apply")), List(frefTree, c.prefix.tree, f.tree, exprGen)))
    reify {
      val silk = e.splice
      silk
    }
  }

  def mapImpl[A, B](c: Context)(f: c.Expr[A => B]) = {
    newOp[A => B, B](c)(c.universe.reify {
      MapOp
    }.tree, f)
  }

  def flatMapImpl[A, B](c: Context)(f: c.Expr[A => SilkOps[B]]) = {
    newOp[A => SilkOps[B], B](c)(c.universe.reify {
      FlatMapOp
    }.tree, f)
  }

  def filterImpl[A](c: Context)(f: c.Expr[A => Boolean]) = {
    newOp[A => Boolean, A](c)(c.universe.reify {
      FilterOp
    }.tree, f)
  }


  def naturalJoinImpl[A: c.WeakTypeTag, B](c: Context)(other: c.Expr[SilkOps[B]])(ev1: c.Expr[scala.reflect.ClassTag[A]], ev2: c.Expr[scala.reflect.ClassTag[B]]): c.Expr[SilkOps[(A, B)]] = {
    import c.universe._

    val helper = new MacroHelper(c)
    val fref = helper.createFContext
    reify {
      JoinOp(fref.splice, c.prefix.splice.asInstanceOf[SilkOps[A]], other.splice)(ev1.splice, ev2.splice)
    }
  }

  def reduceImpl[A](c: Context)(f: c.Expr[(A, A) => A]) = {
    newOp[(A, A) => A, A](c)(c.universe.reify {
      ReduceOp
    }.tree, f)
  }


}

import SilkOps._

/**
 * Function context
 */
case class FContext[A](owner: Class[A], name: String, localValName: Option[String]) {

  def baseTrait : Class[_] = {

    val isAnonFun = owner.getSimpleName.contains("$anonfun")
    if(!isAnonFun)
      owner
    else {
      // The owner is a mix-in class
      owner.getInterfaces.headOption getOrElse owner
    }
  }

  override def toString = {
    s"${baseTrait.getSimpleName}.$name${localValName.map(x => s"#$x") getOrElse ""}"
  }

  def refID: String = s"${owner.getName}#$name"
}



/**
 * Mini-implementation of the Silk framework.
 *
 * SilkOps is an abstraction of operations on data.
 *
 */
abstract class SilkOps[+A: ClassTag](val fref: FContext[_], val uuid: UUID = SilkOps.newUUID) extends Serializable with Logger {

  def inputs: Seq[SilkOps[_]] = Seq.empty
  def getFirstInput: Option[SilkOps[_]] = None

  def idPrefix : String = uuid.toString.substring(0, 8)

  override def toString = {
    val cl = this.getClass
    val schema = ObjectSchema(cl)
    val params = for {p <- schema.constructor.params
                      if p.name != "ss" && p.valueType.rawType != classOf[ClassTag[_]]
                      v = p.get(this) if v != null} yield {
      if (classOf[ru.Expr[_]].isAssignableFrom(p.valueType.rawType)) {
        s"${v}[${v.toString.hashCode}]"
      }
      else if (classOf[SilkOps[_]].isAssignableFrom(p.valueType.rawType)) {
        s"[${v.asInstanceOf[SilkOps[_]].idPrefix}]"
      }
      else
        v
    }

    val prefix = s"[$idPrefix]"
    val s = s"${cl.getSimpleName}(${params.mkString(", ")})"
    val fv = freeVariables
    val fvStr = if (fv == null || fv.isEmpty) "" else s"{${fv.mkString(", ")}}|= "
    s"${
      prefix
    } ${
      fvStr
    }$s"
  }


  def map[B](f: A => B): SilkOps[B] = macro mapImpl[A, B]
  def flatMap[B](f: A => SilkOps[B]): SilkOps[B] = macro flatMapImpl[A, B]
  def filter(f: A => Boolean): SilkOps[A] = macro filterImpl[A]
  def naturalJoin[B](other: SilkOps[B])(implicit ev1: ClassTag[A], ev2: ClassTag[B]): SilkOps[(A, B)] = macro naturalJoinImpl[A, B]
  def reduce[A1 >: A](f:(A1, A1) => A1) : SilkOps[A1] = macro reduceImpl[A1]

  //def eval[A1 >: A](ss:SilkSession): Seq[A1] = slice[A1](ss).flatMap(sl => sl.data)

  @transient val argVariable: Option[ValType] = None
  @transient val freeVariables: Seq[ValType] = Seq.empty

  def enclosingFunctionID: String = fref.refID

}



case class LoadFile[A: ClassTag](override val fref:FContext[_], file:File) extends SilkOps[A](fref)

case class FilterOp[A: ClassTag](override val fref: FContext[_], in: SilkOps[A], f: A => Boolean, @transient fe: ru.Expr[A => Boolean])
  extends SilkOps[A](fref)
  with SplitOp[A => Boolean, A, A] {

}

case class FlatMapOp[A, B: ClassTag](override val fref: FContext[_], in: SilkOps[A], f: A => SilkOps[B], @transient fe: ru.Expr[A => SilkOps[B]])
  extends SilkOps[B](fref)
  with SplitOp[A => SilkOps[B], A, B] {
  private[silk] def fwrap = f.asInstanceOf[Any => SilkOps[Any]]
}


case class MapOp[A, B: ClassTag](override val fref: FContext[_], in: SilkOps[A], f: A => B, @transient fe: ru.Expr[A => B])
  extends SilkOps[B](fref)
  with SplitOp[A => B, A, B] {
  private[silk] def fwrap = f.asInstanceOf[Any => Any]
}
/**
 * @author Taro L. Saito
 */
case class RawSeq[+A: ClassTag](override val fref: FContext[_], @transient in:Seq[A])
  extends SilkOps[A](fref, newUUIDOf(in)) {

}

case class ReduceOp[A: ClassTag](override val fref: FContext[_], in: SilkOps[A], f: (A, A) => A, @transient fe: ru.Expr[(A, A) => A])
  extends SilkOps[A](fref) {

  override def getFirstInput = Some(in)
  override def inputs = Seq(in)
}

/**
 * @author Taro L. Saito
 */
case class ShuffleOp[A: ClassTag, K](override val fref: FContext[_], in: SilkOps[A], keyParam: Parameter, partitioner: K => Int)
  extends SilkOps[A](fref)


/**
 * @author Taro L. Saito
 */
case class MergeShuffleOp[A: ClassTag, B: ClassTag](override val fref: FContext[_], left: SilkOps[A], right: SilkOps[B])
  extends SilkOps[(A, B)](fref) {
  override def inputs = Seq(left, right)
  override def getFirstInput = Some(left)
}

/**
 * @author Taro L. Saito
 */
case class JoinOp[A: ClassTag, B: ClassTag](override val fref: FContext[_], left: SilkOps[A], right: SilkOps[B])
  extends SilkOps[(A, B)](fref) {
  override def inputs = Seq(left, right)
  override def getFirstInput = Some(left)


  def keyParameterPairs = {
    val lt = ObjectSchema.of[A]
    val rt = ObjectSchema.of[B]
    val lp = lt.constructor.params
    val rp = rt.constructor.params
    for (pl <- lp; pr <- rp if (pl.name == pr.name) && pl.valueType == pr.valueType) yield (pl, pr)
  }
}


case class ValType(name: String, tpe: ru.Type) {
  override def toString = s"$name:${
    if (isSilkType) "*" else ""
  }$tpe"
  def isSilkType = {
    import ru._
    tpe <:< typeOf[SilkOps[_]]
  }
}




/**
 * @author Taro L. Saito
 */
trait SplitOp[F, P, A] extends Logger {
  self: SilkOps[A] =>

  import ru._

  val in: SilkOps[P]
  @transient val fe: ru.Expr[F]

  override def getFirstInput = Some(in)
  override def inputs = Seq(in)

  def functionClass: Class[Function1[_, _]] = {
    MacroUtil.mirror.runtimeClass(fe.staticType).asInstanceOf[Class[Function1[_, _]]]
  }

  @transient override val argVariable = {
    fe.tree match {
      case f@Function(List(ValDef(mod, name, e1, e2)), body) =>
        fe.staticType match {
          case TypeRef(prefix, symbol, List(from, to)) =>
            Some(ValType(name.decoded, from))
          case _ => None
        }
      case _ => None
    }
  }

  @transient override val freeVariables = {

    val fvNameSet = (for (v <- fe.tree.freeTerms) yield v.name.decoded).toSet
    val b = Seq.newBuilder[ValType]

    val tv = new Traverser {
      override def traverse(tree: ru.Tree) {
        def matchIdent(idt: Ident): ru.Tree = {
          val name = idt.name.decoded
          if (fvNameSet.contains(name)) {
            val tt: ru.Tree = MacroUtil.toolbox.typeCheck(idt, silent = true)
            b += ValType(idt.name.decoded, tt.tpe)
            tt
          }
          else
            idt
        }

        tree match {
          case idt@Ident(term) =>
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
