package xerial.silk.core
import reflect.ClassTag
import java.io.File

/**
 * A trait for all Silk data types
 * @tparam A
 */
trait Silk[+A] extends SilkOps[A] with Serializable {
 // def eval : Silk[A]
  def isSingle : Boolean
}


/**
 * Silk data class for single elements
 * @tparam A
 */
trait SilkSingle[+A] extends Silk[A] {
///  def map[B](f: A => B) : SilkSingle[B]
  def mapSingle[B](f: A => B) : SilkSingle[B]
  def get: A

  override def isSingle = true
}


object Silk {
  def empty = Empty

  object Empty extends Silk[Nothing] {
    override def isEmpty = true
  }

}


/**
 * A trait that defines silk specific operations
 * @tparam A
 */
trait SilkOps[+A] { self: Silk[A] =>

  import scala.language.experimental.macros

  import SilkFlow._

  private def err = sys.error("N/A")


  def file : SilkSingle[File] = SaveToFile(self)
  def head : SilkSingle[A] = Head(self)

  def foreach[U](f: A => U) : Silk[U] = macro mForeach[A, U]
  def map[B](f: A => B): Silk[B] = macro mMap[A, B]
  def flatMap[B](f: A => Silk[B]): Silk[B] = macro mFlatMap[A, B]
  def filter(p: A => Boolean): Silk[A] = macro mFilter[A]
  def filterNot(p: A => Boolean): Silk[A] = macro mFilterNot[A]
  def withFilter(p: A => Boolean): Silk[A] = macro mWithFilter[A]

  def collect[B](pf: PartialFunction[A, B]): Silk[B] = err
  def collectFirst[B](pf: PartialFunction[A, B]): SilkSingle[Option[B]] = err

  def aggregate[B](z: B)(seqop: (B, A) => B, combop: (B, B) => B): SilkSingle[B] = err
  def reduce[A1 >: A](op: (A1, A1) => A1): SilkSingle[A1] = macro mReduce[A, A1]
  def reduceLeft[B >: A](op: (B, A) => B): SilkSingle[B] = macro mReduceLeft[A, B]
  def fold[A1 >: A](z: A1)(op: (A1, A1) => A1): SilkSingle[A1] = macro mFold[A, A1]
  def foldLeft[B](z: B)(op: (B, A) => B): SilkSingle[B] = macro mFoldLeft[A, B]


  /**
   * Scan the elements with an additional variable z (e.g., a counter) , then produce another Silk data set
   * @param z initial value
   * @param op function updating z and producing another element
   * @tparam B additional variable
   * @tparam C produced element
   */
  def scanLeftWith[B, C](z: B)(op : (B, A) => (B, C)): Silk[C] = ScanLeftWith(self, z, op)

  def size: Long = Count(self).get
  def isSingle: Boolean = false
  def isEmpty: Boolean = size != 0

//  def sum(implicit num: Numeric[A]) = NumericFold(self, num.zero, num.plus)
//  def product[B >: A](implicit num: Numeric[B]) = NumericFold(self, num.one, num.times)
//  def min[A1 >: A](implicit cmp: Ordering[A1]) = NumericReduce(self, (x: A, y: A) => if (cmp.lteq(x, y)) x else y)
//  def max[A1 >: A](implicit cmp: Ordering[A1]) = NumericReduce(self, (x: A, y: A) => if (cmp.gteq(x, y)) x else y)
//  def maxBy[B](f: (A) => B)(implicit cmp: Ordering[B]) = NumericReduce(self, (x: A, y: A) => if (cmp.gteq(f(x), f(y))) x else y)
//  def minBy[B](f: (A) => B)(implicit cmp: Ordering[B]) = NumericReduce(self, (x: A, y: A) => if (cmp.lteq(f(x), f(y))) x else y)

  def mkString(start: String, sep: String, end: String): SilkSingle[String] = MkString(self, start, sep, end)
  def mkString(sep: String): SilkSingle[String] = mkString("", sep, "")
  def mkString: SilkSingle[String] = mkString("")


  def groupBy[K](f: A => K): Silk[(K, Silk[A])] = macro mGroupBy[A, K]

  def join[K, B](other: Silk[B], k1: A => K, k2: B => K): Silk[(K, Silk[(A, B)])] = Join(self, other, k1, k2)
  def joinBy[B](other: Silk[B], cond: (A, B) => Boolean): Silk[(A, B)] = JoinBy(self, other, cond)
  def sortBy[K](keyExtractor: A => K)(implicit ord: Ordering[K]): Silk[A] = SortBy(self, keyExtractor, ord)
  def sorted[A1 >: A](implicit ord: Ordering[A1]): Silk[A1] = Sort[A, A1](self, ord)

  def takeSample(proportion: Double): Silk[A] = Sampling(self, proportion)

  def zip[B](other: Silk[B]) : Silk[(A, B)] = Zip(self, other)
  def zipWithIndex : Silk[(A, Int)] = ZipWithIndex(self)

  // Blocking and its reverse
  def split : Silk[Silk[A]] = Split(self)
  def concat[B](implicit asTraversable: A => Silk[B]) : Silk[B] = Concat(self, asTraversable)

  // Type conversion method
  def toArray[B >: A : ClassTag] : Array[B] = ConvertToArray[A, B](self).get
  def toSeq[B >: A : ClassTag] : Seq[B] = ConvertToSeq[A, B](self).get
  def save : SilkSingle[File] = SaveToFile(self)
}

