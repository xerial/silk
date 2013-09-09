package xerial.silk

import java.util.UUID
import xerial.silk.framework.ops.{LoadFile, SilkMacros, FContext}
import scala.reflect.ClassTag
import scala.language.experimental.macros
import scala.language.existentials
import xerial.silk.framework.IDUtil
import java.io.{ByteArrayOutputStream, ObjectOutputStream, File, Serializable}
import xerial.lens.ObjectSchema
import xerial.silk.SilkException._
import scala.reflect.runtime.{universe=>ru}
import scala.collection.GenTraversable


object Silk {

  def empty[A] = Empty
  private[silk] def emptyFContext = FContext(classOf[Silk[_]], "empty", None)

  object Empty extends SilkSeq[Nothing] {
    def id = UUID.nameUUIDFromBytes("empty".getBytes)
    def fc = emptyFContext
  }

  private[silk] def setEnv(newEnv:SilkEnv) {
    _env = Some(newEnv)
  }

  @transient private var _env : Option[SilkEnv] = None

  def env: SilkEnv = _env.getOrElse {
    SilkException.error("SilkEnv is not yet initialized")
  }

  def loadFile(file:String) : LoadFile = macro SilkMacros.loadImpl

  def newSilk[A](in:Seq[A])(implicit ev:ClassTag[A]) : SilkSeq[A] = macro SilkMacros.mNewSilk[A]

  def scatter[A](in:Seq[A], numNodes:Int)(implicit ev:ClassTag[A]) : SilkSeq[A] = macro SilkMacros.mScatter[A]

  def registerWorkflow[W](name:String, workflow:W) : W ={
    workflow
  }

}



/**
 * Silk[A] is an abstraction of a set of data elements of type A, which might be distributed
 * over cluster nodes.
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

  /**
   * Dependent input Silk data
   * @return
   */
  def inputs : Seq[Silk[_]] = Seq.empty
  def idPrefix = id.prefix

  def save : SilkSingle[File] = NA

  /**
   * Returns Where this Silk operation is defined. A possible value of fc is a variable or a function name.
p   */
  def fc: FContext

  def isSingle : Boolean

  override def toString = {
    val cl = this.getClass
    val schema = ObjectSchema(cl)
    val params = for {p <- schema.constructor.params
                      if p.name != "id" &&  p.name != "ss" && p.valueType.rawType != classOf[ClassTag[_]]
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
 * SilkSeq represents a sequence of elements. Silk data type contains FContext, class and variable names where
 * this SilkSeq is defined. In order to retrieve FContext information,
 * the operators in Silk use Scala macros to inspect the AST of the program code.
 *
 * Since methods defined using macros cannot be called within the same
 * class, each method in Silk must have a separate macro statement.
 *
 */
abstract class SilkSeq[+A] extends Silk[A] {

  import SilkMacros._

  def isSingle = false
  def isEmpty : Boolean = macro mIsEmpty[A]
  def size : SilkSingle[Long] = macro mSize[A]

  // Map with resources
  def mapWith[A, B, R1](r1:Silk[R1])(f: (A, R1) => B) : SilkSeq[B] = macro mMapWith[A, B, R1]
  def mapWith[A, B, R1, R2](r1:Silk[R1], r2:Silk[R2])(f:(A, R1, R2) => B) : SilkSeq[B] = macro mMap2With[A, B, R1, R2]

  // FlatMap with resources
  def flatMapWith[A, B, R1](r1:Silk[R1])(f:(A, R1) => Silk[B]) : SilkSeq[B] = macro mFlatMapWith[A, B, R1]
  def flatMapWith[A, B, R1, R2](r1:Silk[R1], r2:Silk[R2])(f:(A, R1, R2) => Silk[B]) : SilkSeq[B] = macro mFlatMap2With[A, B, R1, R2]

  // For-comprehension
  def foreach[B](f:A=>B) : SilkSeq[B] = macro mForeach[A, B]
  def map[B](f: A => B): SilkSeq[B] = macro mMap[A, B]
  def flatMap[B](f: A => SilkSeq[B]): SilkSeq[B] = macro mFlatMap[A, B]
  def fMap[B](f: A=>GenTraversable[B]) : SilkSeq[B] = macro mFlatMapSeq[A, B]

  // Filtering in for-comprehension
  def filter(cond: A => Boolean): SilkSeq[A] = macro mFilter[A]
  def filterNot(cond: A => Boolean): SilkSeq[A] = macro mFilterNot[A]
  def withFilter(cond: A => Boolean) : SilkSeq[A] = macro mFilter[A] // Use filter

  // Extractor
  def head : SilkSingle[A] = NA
  def collect[B](pf: PartialFunction[A, B]): SilkSeq[B] = NA
  def collectFirst[B](pf: PartialFunction[A, B]): SilkSingle[Option[B]] = NA

  // List operations
  def distinct : SilkSeq[A] = NA

  // Block operations
  def split : SilkSeq[SilkSeq[A]] = macro mSplit[A]
  def concat[B](implicit asSilkSeq: A => Seq[B]) : SilkSeq[B] = macro mConcat[A, B]

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
  def sorted[A1 >: A](partitioner:Partitioner[A])(implicit ord: Ordering[A1]): SilkSeq[A1] = macro mSorted[A1]


  // Operations for gathering distributed data to a node
  /**
   * Collect all distributed data to the node calling this method. This method should be used only for small data.
   */
  def toSeq[A1>:A] : Seq[A1] = get[A1]

  /**
   * Collect all distributed data to the node calling this method. This method should be used only for small data.
   * @tparam A1
   * @return
   */
  def toArray[A1>:A : ClassTag] : Array[A1] = get[A1].toArray

  def get[A1>:A] : Seq[A1] = {
    Silk.env.run(this)
  }

  def get(target:String) : Seq[_] = {
    Silk.env.run(this, target)
  }

  def eval : this.type = {
    Silk.env.eval(this)
    this
  }

}




object SilkSingle {
  import scala.language.implicitConversions
  //implicit def toSilkSeq[A:ClassTag](v:SilkSingle[A]) : SilkSeq[A] = NA
}


/**
 * Silk data class for a single element
 * @tparam A element type
 */
abstract class SilkSingle[+A] extends Silk[A] {

  import SilkMacros._

  def isSingle = true
  def size : Int = 1

  /**
   * Get the materialized result
   */
  def get : A = {
    Silk.env.run(this).head
  }

  def get(target:String) : Seq[_] = {
    Silk.env.run(this, target)
  }

  def map[B](f: A => B): SilkSingle[B] = macro mapSingleImpl[A, B]
  def flatMap[B](f: A => SilkSeq[B]): SilkSeq[B] = macro flatMapSingleImpl[A, B]
  def filter(cond: A => Boolean): SilkSingle[A] = macro filterSingleImpl[A]
  def withFilter(cond: A => Boolean): SilkSingle[A] = macro filterSingleImpl[A]


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
