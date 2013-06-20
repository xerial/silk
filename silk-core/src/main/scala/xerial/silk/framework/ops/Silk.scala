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
import scala.collection.generic.FilterMonadic



/**
 * Function context tells where a silk operation is used.
 */
case class FContext(owner: Class[_], name: String, localValName: Option[String]) {

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

  def empty[A] = Empty
  def emptyFContext = FContext(classOf[Silk[_]], "empty", None)

  object Empty extends SilkSeq[Nothing](emptyFContext)
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

  def save : SilkSingle[File] = NA

  /**
   * Returns Where this Silk operation is defined. A possible value of fc is a variable or a function name.
   */
  def fc: FContext

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


  def toSilkString : String = {
    val s = new StringBuilder
    mkSilkText(0, s)
    s.result.trim
  }


  private def mkSilkText(level:Int, s:StringBuilder)  {
    def indent(n:Int) : String = {
      val b = new StringBuilder(n)
      for(i <- 0 until n) b.append(' ')
      b.result
    }

    val sc = ObjectSchema.apply(getClass)
    val idt = indent(level)
    s.append(s"${idt}-${sc.name}\n")
    for(p <- sc.constructor.params) {
      val v = p.get(this)
      val idt = indent(level+1)
      v match {
        case f:Silk[_] =>
          s.append(s"$idt-${p.name}\n")
          f.mkSilkText(level+2, s)
        case e:ru.Expr[_] =>
          s.append(s"$idt-${p.name}: ${ru.show(e)}\n")
        case _ =>
          s.append(s"$idt-${p.name}: $v\n")
      }
    }
  }

}




/**
 * SilkSeq represents a sequence of elements. In order to retrieve FContext for each operation,
 * Silk operators are implemented by using Scala macros. Since methods defined using macros cannot be called within the same
 * class, each method uses a separate macro statement.
 *
 */
abstract class SilkSeq[+A](val fc: FContext, val id: UUID = Silk.newUUID) extends Silk[A] {

  import SilkMacros._

  def isSingle = false
  def isEmpty = size.get != 0
  def size : SilkSingle[Long] = NA

  // For-comprehension
  def foreach[B](f:A=>B) : SilkSeq[B] = macro mForeach[A, B]
  def map[B](f: A => B): SilkSeq[B] = macro mMap[A, B]
  def flatMap[B](f: A => SilkSeq[B]): SilkSeq[B] = macro mFlatMap[A, B]

  // Filtering
  def filter(f: A => Boolean): SilkSeq[A] = macro mFilter[A]
  def filterNot(f: A => Boolean): SilkSeq[A] = macro mFilterNot[A]
  def withFilter(cond: A => Boolean) : SilkSeq[A] = NA

  // Extractor
  def head : SilkSingle[A] = NA
  def collect[B](pf: PartialFunction[A, B]): SilkSeq[B] = NA
  def collectFirst[B](pf: PartialFunction[A, B]): SilkSingle[Option[B]] = NA

  // List operations
  def distinct : SilkSeq[A] = NA

  // Block operations
  def split : SilkSeq[SilkSeq[A]] = macro mSplit[A]
  def concat[B](implicit asSilkSeq: A => SilkSeq[B]) : SilkSeq[B] = macro mConcat[A, B]

  // Grouping
  def groupBy[K](f: A => K): SilkSeq[(K, SilkSeq[A])] = macro mGroupBy[A, K]


  // Aggregators
  def aggregate[B](z: B)(seqop: (B, A) => B, combop: (B, B) => B): SilkSingle[B] = NA
  def reduce[A1 >: A](f:(A1, A1) => A1) : SilkSingle[A1] = macro mReduce[A1]
  def reduceLeft[B >: A](op: (B, A) => B): SilkSingle[B] = NA // macro mReduceLeft[A, B]
  def fold[A1 >: A](z: A1)(op: (A1, A1) => A1): SilkSingle[A1] = NA // macro mFold[A, A1]
  def foldLeft[B](z: B)(op: (B, A) => B): SilkSingle[B] = NA // macro mFoldLeft[A, B]

  // Scan operations
  /**
   * Scan the elements with an additional variable z (e.g., a counter) , then produce another Silk data set
   * @param z initial value
   * @param op function that produces a pair (new z, another element)
   * @tparam B additional variable (e.g., counter)
   * @tparam C produced element
   */
  def scanLeftWith[B, C](z: B)(op : (B, A) => (B, C)): SilkSeq[C] = NA



  // Joins
  def naturalJoin[B](other: SilkSeq[B])(implicit ev1: ClassTag[A], ev2: ClassTag[B]): SilkSeq[(A, B)] = macro mNaturalJoin[A, B]
  def join[K, B](other: SilkSeq[B], k1: A => K, k2: B => K) = macro mJoin[A, K, B]
  //def joinBy[B](other: SilkSeq[B], cond: (A, B) => Boolean) = macro mJoinBy[A, B]


  // Numeric operation
  def sum[A1>:A](implicit num: Numeric[A1]) : SilkSingle[A1] = macro mSum[A1]
  def product[A1 >: A](implicit num: Numeric[A1]) = macro mProduct[A1]
  def min[A1 >: A](implicit cmp: Ordering[A1]) = macro mMin[A1]
  def max[A1 >: A](implicit cmp: Ordering[A1]) = macro mMax[A1]
  def minBy[A1 >: A, B](f: (A1) => B)(implicit cmp: Ordering[B]) = macro mMinBy[A1, B]
  def maxBy[A1 >: A, B](f: (A1) => B)(implicit cmp: Ordering[B]) = macro mMaxBy[A1, B]


  // String
  def mkString(start: String, sep: String, end: String): SilkSingle[String] = macro mMkString[A]
  def mkString(sep: String): SilkSingle[String] = macro mMkStringSep[A]
  def mkString: SilkSingle[String] = macro mMkStringDefault[A]


  // Sampling
  def takeSample(proportion:Double) : SilkSeq[A] = macro mSampling[A]

  // Zipper
  def zip[B](other:SilkSeq[B]) : SilkSeq[(A, B)] = macro mZip[A, B]
  def zipWithIndex : SilkSeq[(A, Int)] = macro mZipWithIndex[A]

  // Sorting
  def sortBy[K](keyExtractor: A => K)(implicit ord: Ordering[K]): SilkSeq[A] = macro mSortBy[A, K]
  def sorted[A1 >: A](implicit ord: Ordering[A1]): SilkSeq[A1] = macro mSorted[A1]


  // Operations for gathering distributed data to a node
  def toSeq[A1>:A] : Seq[A1] = NA
  def toArray[A1>:A : ClassTag] : Array[A1] = NA

}




object SilkSingle {
  import scala.language.implicitConversions
  implicit def toSilkSeq[A:ClassTag](v:SilkSingle[A]) : SilkSeq[A] = NA
}


/**
 * Silk data class for a single element
 * @tparam A element type
 */
abstract class SilkSingle[+A](val fc:FContext, val id: UUID = Silk.newUUID) extends Silk[A] {

  import SilkMacros._

  def isSingle = true
  def size : Int = 1

  /**
   * Get the materialized result
   */
  def get : A = NA // TODO impl

  def map[B](f: A => B): SilkSingle[B] = macro mapSingleImpl[A, B]
  def flatMap[B](f: A => SilkSeq[B]): SilkSeq[B] = macro flatMapSingleImpl[A, B]
  def filter(f: A => Boolean): SilkSingle[A] = macro filterSingleImpl[A]


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
