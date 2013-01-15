//--------------------------------------
//
// Silk.scala
// Since: 2012/11/30 2:33 PM
//
//--------------------------------------

package xerial.silk.core

import java.io.{ByteArrayInputStream, ObjectInputStream, ByteArrayOutputStream, ObjectOutputStream}
import collection.mutable.{Builder, ArraySeq}
import scala.Iterator
import scala.Seq
import scala.Iterable
import scala.Ordering
import scala.util.Random
import scala.Some
import collection.{TraversableOnce, GenTraversableOnce, GenTraversable}
import collection.generic.CanBuildFrom
import xerial.silk.cluster.ClusterCommand
import xerial.silk.cluster.SilkClient.ClientInfo


/**
 * @author Taro L. Saito
 */
object Silk {

  def hosts : Silk[ClientInfo] = {
    val hosts = new ClusterCommand().listServerStatus map { case (ci, status) =>
      ci
    }
    new SilkInMemory(hosts.toSeq)
  }


  def fromFile[A](path:String) = new SilkFileSource(path)

  def toSilk[A](obj: A): Silk[A] = {
    new SilkInMemory[A](Seq(obj))
  }

  def toSilkSeq[A](a:Seq[A]) : Silk[A] = {
    new SilkInMemory(a)
  }

  def toSilkArray[A](a:Array[A]) : Silk[A] = {
    // TODO optimization
    new SilkInMemory(a.toSeq)
  }

  object Empty extends Silk[Nothing] with SilkStandardImpl[Nothing] {
    def newBuilder[T] = SilkInMemory.newBuilder[T]
    def iterator = Iterator.empty
    def eval = this
  }

  def single[A](e:A) : SilkSingle[A] = new SilkSingleImpl(e)

  object EmptySingle extends SilkSingle[Nothing] with SilkStandardImpl[Nothing] {
    def iterator = Iterator.empty
    def newBuilder[T] = SilkInMemory.newBuilder[T]
    def mapSingle[B](f: (Nothing) => B) = EmptySingle
    def get = null.asInstanceOf[Nothing]
  }

  private class SilkSingleImpl[A](a:A) extends SilkSingle[A] with SilkStandardImpl[A] {
    def iterator = Iterator.single(a)
    def newBuilder[T] = SilkInMemory.newBuilder[T]
    override def toString = a.toString
    def mapSingle[B](f: (A) => B) = single(f(a))
    def eval = this
    def get = a
  }
}


/**
 * A trait for all Silk data types
 * @tparam A
 */
trait Silk[+A] extends SilkOps[A] with Serializable {
 // def eval : Silk[A]
}

/**
 * Silk data class for single elements
 * @tparam A
 */
trait SilkSingle[+A] extends Silk[A] {
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

  def iterator: Iterator[A]

  def newBuilder[T]: Builder[T, Silk[T]]

  def foreach[U](f: A => U) : Silk[U]
  def map[B](f: A => B): Silk[B]
  def flatMap[B](f: A => GenTraversableOnce[B]): Silk[B]

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


  def withFilter(p: A => Boolean): SilkMonadicFilter[A]

  def zip[B](other: Silk[B]) : Silk[(A, B)]
  def zipWithIndex : Silk[(A, Int)]

  def concat[B](implicit asTraversable: A => Silk[B]) : Silk[B]

  // Type conversion method
  def toArray[B >: A : ClassManifest] : Array[B]

  def save[B >:A] : Silk[B]


}

/**
 * A trait for supporting for(x <- Silk[A] if cond) syntax
 * @tparam A
 */
trait SilkMonadicFilter[+A] extends Silk[A] with SilkStandardImpl[A] {
  def map[B](f: A => B): Silk[B]
  def flatMap[B](f: A => collection.GenTraversableOnce[B]): Silk[B]
  def foreach[U](f: A => U): Silk[U]
  def withFilter(p: A => Boolean): SilkMonadicFilter[A]
}



