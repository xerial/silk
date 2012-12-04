//--------------------------------------
//
// Silk.scala
// Since: 2012/11/30 2:33 PM
//
//--------------------------------------

package xerial.silk.collection

import java.io.{ByteArrayInputStream, ObjectInputStream, ByteArrayOutputStream, ObjectOutputStream}
import collection.mutable.{Builder, ArraySeq}
import scala.Iterator
import scala.Seq
import scala.Iterable
import scala.Ordering
import scala.util.Random
import scala.Some
import collection.{GenTraversableOnce, GenTraversable}
import collection.generic.CanBuildFrom


/**
 * @author Taro L. Saito
 */
object Silk {

  def toSilk[A](obj: A): Silk[A] = {
    new InMemorySilk[A](Seq(obj))
  }

  object Empty extends Silk[Nothing] with SilkLike[Nothing] {
    def newBuilder[T] = InMemorySilk.newBuilder[T]
    def iterator = Iterator.empty
  }

}


/**
 * A trait for all Silk data types
 * @tparam A
 */
trait Silk[+A] extends GenSilk[A] {

}


trait ObjectMapping[-A, +B] {
  def apply(e: A): B
}

/**
 * A common trait for implementing silk operations
 * @tparam A
 */
trait GenSilk[+A]
  extends SilkOps[A] {

}


/**
 * A trait that defines silk specific operations
 * @tparam A
 */
trait SilkOps[+A] {

  def iterator: Iterator[A]

  def newBuilder[T]: Builder[T, Silk[T]]

  def foreach[U](f: A => U)
  def map[B](f: A => B): Silk[B]
  def flatMap[B](f: A => GenTraversableOnce[B]): Silk[B]

  def filter(p: A => Boolean): Silk[A]
  def filterNot(p: A => Boolean): Silk[A] = filter({
    x => !p(x)
  })

  def collect[B](pf: PartialFunction[A, B]): Silk[B]
  def collectFirst[B](pf: PartialFunction[A, B]): Option[B]

  def aggregate[B](z: B)(seqop: (B, A) => B, combop: (B, B) => B): B
  def reduce[A1 >: A](op: (A1, A1) => A1): A1
  def reduceLeft[B >: A](op: (B, A) => B): B
  def fold[A1 >: A](z: A1)(op: (A1, A1) => A1): A1
  def foldLeft[B](z: B)(op: (B, A) => B): B


  def size: Int
  def isSingle: Boolean
  def isEmpty: Boolean

  def sum[B >: A](implicit num: Numeric[B]): B
  def product[B >: A](implicit num: Numeric[B]): B
  def min[B >: A](implicit cmp: Ordering[B]): A
  def max[B >: A](implicit cmp: Ordering[B]): A
  def maxBy[B](f: A => B)(implicit cmp: Ordering[B]): A
  def minBy[B](f: A => B)(implicit cmp: Ordering[B]): A

  def mkString(start: String, sep: String, end: String): String;
  def mkString(sep: String): String = mkString("", sep, "")
  def mkString: String = mkString("")


  def groupBy[K](f: A => K): Silk[(K, Silk[A])]


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
}

/**
 * A trait for supporting for(x <- Silk[A] if cond) syntax
 * @tparam A
 */
trait SilkMonadicFilter[+A] {
  def map[B](f: A => B): Silk[B]
  def flatMap[B](f: A => collection.GenTraversableOnce[B]): Silk[B]
  def foreach[U](f: A => U): Unit
  def withFilter(p: A => Boolean): SilkMonadicFilter[A]
}




