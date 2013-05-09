package xerial.silk.core
import reflect.ClassTag

/**
 * A trait for all Silk data types
 * @tparam A
 */
trait Silk[+A] extends SilkOps[A] with Serializable {
 // def eval : Silk[A]
}

/**
 * @author Taro L. Saito
 */
object Silk {


  object Empty extends Silk[Nothing] {
    def eval = this
    def foreach[U](f: (Nothing) => U) = Empty
    def map[B](f: (Nothing) => B) = Empty
    def flatMap[B](f: (Nothing) => Silk[B]) = Empty
    def filter(p: (Nothing) => Boolean) = Empty
    def collect[B](pf: PartialFunction[Nothing, B]) = Empty
    def collectFirst[B](pf: PartialFunction[Nothing, B]) = EmptySingle
    def aggregate[B](z: B)(seqop: (B, Nothing) => B, combop: (B, B) => B) = EmptySingle
    def reduce[A1 >: Nothing](op: (A1, A1) => A1) = EmptySingle
    def reduceLeft[B >: Nothing](op: (B, Nothing) => B) = EmptySingle
    def fold[A1 >: Nothing](z: A1)(op: (A1, A1) => A1) = EmptySingle
    def foldLeft[B](z: B)(op: (B, Nothing) => B) = EmptySingle
    def head = EmptySingle
    def scanLeftWith[B, C](z: B)(op: (B, Nothing) => (B, C)) = EmptySingle
    def size = 0
    def isSingle = false
    def isEmpty = true
    def sum[B >: Nothing](implicit num: Numeric[B]) = EmptySingle
    def product[B >: Nothing](implicit num: Numeric[B]) = EmptySingle
    def min[B >: Nothing](implicit cmp: Ordering[B]) = EmptySingle
    def max[B >: Nothing](implicit cmp: Ordering[B]) = EmptySingle
    def maxBy[B](f: (Nothing) => B)(implicit cmp: Ordering[B]) = EmptySingle
    def minBy[B](f: (Nothing) => B)(implicit cmp: Ordering[B]) = EmptySingle
    def mkString(start: String, sep: String, end: String) = EmptySingle
    def groupBy[K](f: (Nothing) => K) = Empty
    def split = Empty
    def project[B](implicit mapping: ObjectMapping[Nothing, B]) = Empty
    def join[K, B](other: Silk[B], k1: (Nothing) => K, k2: (B) => K) = Empty
    def joinBy[B](other: Silk[B], cond: (Nothing, B) => Boolean) = Empty
    def sortBy[K](keyExtractor: (Nothing) => K)(implicit ord: Ordering[K]) = Empty
    def sorted[A1 >: Nothing](implicit ord: Ordering[A1]) = Empty
    def takeSample(proportion: Double) = Empty
    def withFilter(p: (Nothing) => Boolean) = Empty
    def zip[B](other: Silk[B]) = Empty
    def zipWithIndex = Empty
    def concat[B](implicit asTraversable: (Nothing) => Silk[B]) = Empty
    def toArray[B >: Nothing : ClassTag] = Array.empty[B]
    def save[B >: Nothing] = EmptySingle
  }

  //def single[A](e:A) : SilkSingle[A] = new SilkSingleImpl(e)

  object EmptySingle extends SilkSingle[Nothing]  {
    override def map[B](f: (Nothing) => B) : SilkSingle[B] = EmptySingle
    def mapSingle[B](f: (Nothing) => B) = EmptySingle
    def get = null.asInstanceOf[Nothing]
    def foreach[U](f: (Nothing) => U) = EmptySingle
    def flatMap[B](f: (Nothing) => Silk[B]) = EmptySingle
    def filter(p: (Nothing) => Boolean) = EmptySingle
    def collect[B](pf: PartialFunction[Nothing, B]) = EmptySingle
    def collectFirst[B](pf: PartialFunction[Nothing, B]) = EmptySingle
    def aggregate[B](z: B)(seqop: (B, Nothing) => B, combop: (B, B) => B) = EmptySingle
    def reduce[A1 >: Nothing](op: (A1, A1) => A1) = EmptySingle
    def reduceLeft[B >: Nothing](op: (B, Nothing) => B) = EmptySingle
    def fold[A1 >: Nothing](z: A1)(op: (A1, A1) => A1) = EmptySingle
    def foldLeft[B](z: B)(op: (B, Nothing) => B) = EmptySingle
    def head = EmptySingle
    def scanLeftWith[B, C](z: B)(op: (B, Nothing) => (B, C)) = EmptySingle
    def size = 0
    def isSingle = true
    def isEmpty = true
    def sum[B >: Nothing](implicit num: Numeric[B]) = EmptySingle
    def product[B >: Nothing](implicit num: Numeric[B]) = EmptySingle
    def min[B >: Nothing](implicit cmp: Ordering[B]) = EmptySingle
    def max[B >: Nothing](implicit cmp: Ordering[B]) = EmptySingle
    def maxBy[B](f: (Nothing) => B)(implicit cmp: Ordering[B]) = EmptySingle
    def minBy[B](f: (Nothing) => B)(implicit cmp: Ordering[B]) = EmptySingle
    def mkString(start: String, sep: String, end: String) = EmptySingle
    def groupBy[K](f: (Nothing) => K) = EmptySingle
    def split = EmptySingle
    def project[B](implicit mapping: ObjectMapping[Nothing, B]) = EmptySingle
    def join[K, B](other: Silk[B], k1: (Nothing) => K, k2: (B) => K) = EmptySingle
    def joinBy[B](other: Silk[B], cond: (Nothing, B) => Boolean) = EmptySingle
    def sortBy[K](keyExtractor: (Nothing) => K)(implicit ord: Ordering[K]) = EmptySingle
    def sorted[A1 >: Nothing](implicit ord: Ordering[A1]) = EmptySingle
    def takeSample(proportion: Double) = EmptySingle
    def withFilter(p: (Nothing) => Boolean) = EmptySingle
    def zip[B](other: Silk[B]) = EmptySingle
    def zipWithIndex = EmptySingle
    def concat[B](implicit asTraversable: (Nothing) => Silk[B]) = EmptySingle
    def toArray[B >: Nothing : ClassTag] = Array.empty[B]
    def save[B >: Nothing] = EmptySingle
  }

//  private class SilkSingleImpl[A](a:A) extends SilkSingle[A] {
//    override def toString = a.toString
//    override def map[B](f: A => B) : SilkSingle[B] = mapSingle(f)
//    def mapSingle[B](f: (A) => B) = single(f(a))
//    def eval = this
//    def get = a
//  }
}
/**
 * Silk data class for single elements
 * @tparam A
 */
trait SilkSingle[+A] extends Silk[A] {
  def map[B](f: A => B) : SilkSingle[B]
  def mapSingle[B](f: A => B) : SilkSingle[B]
  def get: A
}
/**
 * For taking projections of Silk data
 * @tparam A
 * @tparam B
 */
trait ObjectMapping[-A, +B] {
  def apply(e: A): B
}
/**
 * A trait that defines silk specific operations
 * @tparam A
 */
trait SilkOps[+A] {

  def foreach[U](f: A => U) : Silk[U]

  //def project[B](f: A => B) : Silk[B]
  def map[B](f: A => B): Silk[B]
  def flatMap[B](f: A => Silk[B]): Silk[B]

  def filter(p: A => Boolean): Silk[A]
  def filterNot(p: A => Boolean): Silk[A] = filter({
    x => !p(x)
  })

  def collect[B](pf: PartialFunction[A, B]): Silk[B]
  def collectFirst[B](pf: PartialFunction[A, B]): SilkSingle[Option[B]]

  def aggregate[B](z: B)(seqop: (B, A) => B, combop: (B, B) => B): SilkSingle[B]
  def reduce[A1 >: A](op: (A1, A1) => A1): SilkSingle[A1]
  def reduceLeft[B >: A](op: (B, A) => B): SilkSingle[B]
  def fold[A1 >: A](z: A1)(op: (A1, A1) => A1): SilkSingle[A1]
  def foldLeft[B](z: B)(op: (B, A) => B): SilkSingle[B]

  def head : SilkSingle[A]

  /**
   * Scan the elements with an additional variable z (e.g., counter) , then produce another Silk data set
   * @param z initial value
   * @param op function updating z and producing another element
   * @tparam B additional variable
   * @tparam C produced element
   */
  def scanLeftWith[B, C](z: B)(op : (B, A) => (B, C)): Silk[C]


  def size: Int
  def isSingle: Boolean
  def isEmpty: Boolean

  def sum[B >: A](implicit num: Numeric[B]): SilkSingle[B]
  def product[B >: A](implicit num: Numeric[B]): SilkSingle[B]
  def min[B >: A](implicit cmp: Ordering[B]): SilkSingle[A]
  def max[B >: A](implicit cmp: Ordering[B]): SilkSingle[A]
  def maxBy[B](f: A => B)(implicit cmp: Ordering[B]): SilkSingle[A]
  def minBy[B](f: A => B)(implicit cmp: Ordering[B]): SilkSingle[A]

  def mkString(start: String, sep: String, end: String): SilkSingle[String];
  def mkString(sep: String): SilkSingle[String] = mkString("", sep, "")
  def mkString: SilkSingle[String] = mkString("")


  def groupBy[K](f: A => K): Silk[(K, Silk[A])]

  def split : Silk[Silk[A]]


  /**
   * Extract a projection B of A. This function is used to extract a sub set of
   * columns(parameters)
   * @tparam B target object
   * @return
   */
  def project[B](implicit mapping: ObjectMapping[A, B]): Silk[B]
  def join[K, B](other: Silk[B], k1: A => K, k2: B => K): Silk[(K, Silk[(A, B)])]
  def joinBy[B](other: Silk[B], cond: (A, B) => Boolean): Silk[(A, B)]
  def sortBy[K](keyExtractor: A => K)(implicit ord: Ordering[K]): Silk[A]
  def sorted[A1 >: A](implicit ord: Ordering[A1]): Silk[A1]

  def takeSample(proportion: Double): Silk[A]


  def withFilter(p: A => Boolean): Silk[A]

  def zip[B](other: Silk[B]) : Silk[(A, B)]
  def zipWithIndex : Silk[(A, Int)]

  def concat[B](implicit asTraversable: A => Silk[B]) : Silk[B]

  // Type conversion method
  def toArray[B >: A : ClassTag] : Array[B]
  def toSeq[B >: A : ClassTag] = toArray[B].toSeq
  def save[B >:A] : Silk[B]
}
/**
 * A trait for supporting for(x <- Silk[A] if cond) syntax
 * @tparam A
 */
trait SilkMonadicFilter[+A] extends Silk[A] {
  def map[B](f: A => B): Silk[B]
  def flatMap[B](f: A => Silk[B]): Silk[B]
  def foreach[U](f: A => U): Silk[U]
  def withFilter(p: A => Boolean): SilkMonadicFilter[A]
}