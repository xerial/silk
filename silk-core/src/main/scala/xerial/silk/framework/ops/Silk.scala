//--------------------------------------
//
// Silk.scala
// Since: 2013/06/19 4:52 PM
//
//--------------------------------------

package xerial.silk.framework.ops

import scala.language.experimental.macros
import scala.language.existentials
import xerial.silk.framework.IDUtil
import java.util.UUID
import scala.reflect.ClassTag
import java.io.{ByteArrayOutputStream, ObjectOutputStream, File, Serializable}
import xerial.lens.{Parameter, ObjectSchema}
import scala.reflect.runtime.{universe=>ru}
import xerial.silk.SilkException._
import xerial.core.io.text.UString


/**
 * Function context tells where a silk operation is used.
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


case class ValType(name: String, tpe: ru.Type) {
  override def toString = s"$name:${
    if (isSilkType) "*" else ""
  }$tpe"
  def isSilkType = {
    import ru._
    tpe <:< typeOf[Silk[_]]
  }
}


object Silk {

  def newUUID: UUID = UUID.randomUUID

  def newUUIDOf[A](in: Seq[A]): UUID = {
    val b = new ByteArrayOutputStream
    val os = new ObjectOutputStream(b)
    for (x <- in)
      os.writeObject(x)
    os.close
    UUID.nameUUIDFromBytes(b.toByteArray)
  }

}


/**
 * Silk[A] is an abstraction of a set of data elements of type A, which is distributed
 * over cluster machines.
 *
 * Silk[A] is a base trait for all Silk data types.
 *
 * We do not define operations on Silk data in this trait,
 * since map, filter etc. needs macro-based implementation, which cannot be used to override
 * interface methods.
 *
 * @tparam A element type
 */
trait Silk[+A] extends Serializable with IDUtil {

  def id: UUID
  def inputs : Seq[Silk[_]] = Seq.empty
  def idPrefix = id.prefix

  def save : this.type = NA

  /**
   * Returns Where this Silk operation is defined. A possible value of fc is a variable or a function name.
   */
  def fc: FContext[_]

  def isSingle : Boolean

  override def toString = {
    val cl = this.getClass
    val schema = ObjectSchema(cl)
    val params = for {p <- schema.constructor.params
                      if p.name != "ss" && p.valueType.rawType != classOf[ClassTag[_]]
                      v = p.get(this) if v != null} yield {
      if (classOf[ru.Expr[_]].isAssignableFrom(p.valueType.rawType)) {
        s"${v}[${v.toString.hashCode}]"
      }
      else if (classOf[Silk[_]].isAssignableFrom(p.valueType.rawType)) {
        s"[${v.asInstanceOf[Silk[_]].idPrefix}]"
      }
      else
        v
    }

    val prefix = s"[$idPrefix]"
    val s = s"${cl.getSimpleName}(${params.mkString(", ")})"
//    val fv = freeVariables
//    val fvStr = if (fv == null || fv.isEmpty) "" else s"{${fv.mkString(", ")}}|= "
    s"$prefix $s"
  }

}




/**
 * SilkSeq represents a sequence of elements
 *
 */
abstract class SilkSeq[+A: ClassTag](val fc: FContext[_], val id: UUID = Silk.newUUID) extends Silk[A] {

  import SilkMacros._

  def isSingle = false

  def size : SilkSingle[Long] = NA

  def map[B](f: A => B): SilkSeq[B] = macro mapImpl[A, B]
  def flatMap[B](f: A => SilkSeq[B]): SilkSeq[B] = macro flatMapImpl[A, B]
  def filter(f: A => Boolean): SilkSeq[A] = macro filterImpl[A]
  def naturalJoin[B](other: SilkSeq[B])(implicit ev1: ClassTag[A], ev2: ClassTag[B]): SilkSeq[(A, B)] = macro naturalJoinImpl[A, B]
  def reduce[A1 >: A](f:(A1, A1) => A1) : SilkSingle[A1] = macro reduceImpl[A1]
  def zip[B](other:SilkSeq[B]) : SilkSeq[(A, B)] = NA

  def head : SilkSingle[A] = NA

  // Block operations
  def split : SilkSeq[SilkSeq[A]] = NA
  def concat[B](implicit asTraversable: A => SilkSeq[B]) : SilkSeq[B] = NA

  // List operations
  def distinct : SilkSeq[A] = NA
  def collect[B](pf: PartialFunction[A, B]): SilkSeq[B] = NA
  def collectFirst[B](pf: PartialFunction[A, B]): SilkSingle[Option[B]] = NA

  // Scan operations
  /**
   * Scan the elements with an additional variable z (e.g., a counter) , then produce another Silk data set
   * @param z initial value
   * @param op function that produces a pair (new z, another element)
   * @tparam B additional variable (e.g., counter)
   * @tparam C produced element
   */
  def scanLeftWith[B, C](z: B)(op : (B, A) => (B, C)): SilkSeq[C] = NA


  // Sorting
  def sortBy[K](keyExtractor: A => K)(implicit ord: Ordering[K]): SilkSeq[A] = NA
  def sorted[A1 >: A](implicit ord: Ordering[A1]): SilkSeq[A1] = NA


  // Numeric operation
  def sum[A1>:A](implicit num: Numeric[A1]) : SilkSingle[A1] = NA

  def toSeq[A1>:A] : Seq[A1] = NA
  def toArray[A1>:A : ClassTag] : Array[A1] = NA


  // String
  def mkString(sep:String) : String = NA

}


case class LoadFile(override val fc:FContext[_], file:File) extends SilkSingle[File](fc) {
  def lines : SilkSeq[String] = NA
  def rawLines : SilkSeq[UString] = NA
  def as[A](implicit ev:ClassTag[A]) : SilkSeq[A] = NA
}

case class FilterOp[A: ClassTag](override val fc: FContext[_], in: SilkSeq[A], f: A => Boolean, @transient fe: ru.Expr[A => Boolean])
  extends SilkSeq[A](fc)

case class FlatMapOp[A, B: ClassTag](override val fc: FContext[_], in: SilkSeq[A], f: A => SilkSeq[B], @transient fe: ru.Expr[A => SilkSeq[B]])
  extends SilkSeq[B](fc) {

  def fwrap = f.asInstanceOf[Any => SilkSeq[Any]]
}

case class MapOp[A, B: ClassTag](override val fc: FContext[_], in: SilkSeq[A], f: A => B, @transient fe: ru.Expr[A => B])
  extends SilkSeq[B](fc) {

  def fwrap = f.asInstanceOf[Any => Any]

}

case class RawSeq[+A: ClassTag](override val fc: FContext[_], @transient in:Seq[A])
  extends SilkSeq[A](fc)

case class ShuffleOp[A: ClassTag, K](override val fc: FContext[_], in: SilkSeq[A], keyParam: Parameter, partitioner: K => Int)
  extends SilkSeq[A](fc)


case class MergeShuffleOp[A: ClassTag, B: ClassTag](override val fc: FContext[_], left: SilkSeq[A], right: SilkSeq[B])
  extends SilkSeq[(A, B)](fc) {
  override def inputs = Seq(left, right)
}

case class JoinOp[A: ClassTag, B: ClassTag](override val fc: FContext[_], left: SilkSeq[A], right: SilkSeq[B])
  extends SilkSeq[(A, B)](fc) {
  override def inputs = Seq(left, right)

  def keyParameterPairs = {
    val lt = ObjectSchema.of[A]
    val rt = ObjectSchema.of[B]
    val lp = lt.constructor.params
    val rp = rt.constructor.params
    for (pl <- lp; pr <- rp if (pl.name == pr.name) && pl.valueType == pr.valueType) yield (pl, pr)
  }
}


/**
 * Silk data class for a single element
 * @tparam A element type
 */
abstract class SilkSingle[+A: ClassTag](val fc:FContext[_], val id: UUID = Silk.newUUID) extends Silk[A] {

  import SilkMacros._

  def isSingle = true

  def size : Int = 1

  def get : A = NA // TODO impl

  // Numeric operation
  def /[A1>:A](implicit num: Numeric[A1]) : SilkSingle[A1] = NA

  def map[B](f: A => B): SilkSingle[B] = macro mapSingleImpl[A, B]
  def flatMap[B](f: A => SilkSeq[B]): SilkSeq[B] = macro flatMapImpl[A, B]
  def filter(f: A => Boolean): SilkSingle[A] = macro filterSingleImpl[A]


}

case class MapSingleOp[A, B : ClassTag](override val fc: FContext[_], in:SilkSingle[A], f: A=>B, @transient fe: ru.Expr[A=>B])
 extends SilkSingle[B](fc)

case class FilterSingleOp[A: ClassTag](override val fc: FContext[_], in:SilkSingle[A], f: A=>Boolean, @transient fe: ru.Expr[A=>Boolean])
  extends SilkSingle[A](fc)


case class SilkEmpty(override val fc:FContext[_]) extends SilkSingle[Nothing](fc) {
  override def size = 0

}
case class ReduceOp[A: ClassTag](override val fc: FContext[_], in: SilkSeq[A], f: (A, A) => A, @transient fe: ru.Expr[(A, A) => A])
  extends SilkSingle[A](fc) {
  override def inputs = Seq(in)
}



//
//
///**
// * @author Taro L. Saito
// */
//trait FunctionHelper[F, P, A] extends Logger {
//  self: SilkSeq[A] =>
//
//  import ru._
//
//  val in: SilkOps[P]
//  @transient val fe: ru.Expr[F]
//
//  override def getFirstInput = Some(in)
//  override def inputs = Seq(in)
//
//  def functionClass: Class[Function1[_, _]] = {
//    MacroUtil.mirror.runtimeClass(fe.staticType).asInstanceOf[Class[Function1[_, _]]]
//  }
//
//  @transient override val argVariable = {
//    fe.tree match {
//      case f@Function(List(ValDef(mod, name, e1, e2)), body) =>
//        fe.staticType match {
//          case TypeRef(prefix, symbol, List(from, to)) =>
//            Some(ValType(name.decoded, from))
//          case _ => None
//        }
//      case _ => None
//    }
//  }
//
//  @transient override val freeVariables = {
//
//    val fvNameSet = (for (v <- fe.tree.freeTerms) yield v.name.decoded).toSet
//    val b = Seq.newBuilder[ValType]
//
//    val tv = new Traverser {
//      override def traverse(tree: ru.Tree) {
//        def matchIdent(idt: Ident): ru.Tree = {
//          val name = idt.name.decoded
//          if (fvNameSet.contains(name)) {
//            val tt: ru.Tree = MacroUtil.toolbox.typeCheck(idt, silent = true)
//            b += ValType(idt.name.decoded, tt.tpe)
//            tt
//          }
//          else
//            idt
//        }
//
//        tree match {
//          case idt@Ident(term) =>
//            matchIdent(idt)
//          case other => super.traverse(other)
//        }
//      }
//    }
//    tv.traverse(fe.tree)
//
//    // Remove duplicate occurrences.
//    b.result.distinct
//  }
//
//}
