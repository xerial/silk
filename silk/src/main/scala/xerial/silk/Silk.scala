//--------------------------------------
//
// Silk.scala
// Since: 2012/11/30 2:33 PM
//
//--------------------------------------

package xerial.silk

import java.io.{ByteArrayInputStream, ObjectInputStream, ByteArrayOutputStream, ObjectOutputStream}
import collection._
import collection.generic.CanBuildFrom
import scala.Iterator
import scala.Seq
import scala.Iterable

/**
 * @author Taro L. Saito
 */
object Silk {

  def toSilk[A](obj:A) : Silk[A] = {
    new InMemorySilk[A](Seq(obj))
  }

  object Empty extends Silk[Nothing] {
    def isEmpty = true
    def size = 0
    def map[B](f: (Nothing) => B) = Empty
    def filter(f: (Nothing) => Boolean) = Empty
    def reduce(f: (Nothing, Nothing) => Nothing) = Empty
    def fold[B](z: B)(f: (B, Nothing) => B) = Empty
    def foreach[U](f: (Nothing) => U) {}
    def flatMap[B](f: (Nothing) => Silk[B]) = Empty
    def project[B] = Empty
    def collect[B](pf: PartialFunction[Nothing, B]) = Empty
    def reduce[A1 >: Nothing](f: (A1, A1) => A1) = Empty
    def fold[A1 >: Nothing](z: A1)(f: (A1, A1) => A1) = Empty
    def forall(pred: (Nothing) => Boolean) = false
    def groupBy[K](keyExtractor: Nothing => K) = Empty
    def join[K, B](other: Silk[B], k1: (Nothing) => K, k2: (B) => K) = Empty
    def joinBy[B](other: Silk[B], cond: (Nothing, B) => Boolean) = Empty
    def sortBy[K](keyExtractor: (Nothing) => K)(implicit ord: Ordering[K]) = Empty
    def sortBy(ord: Ordering[Nothing]) = Empty
    def takeSample(proportion: Double) = Empty
    def iterator = Iterator.empty
    def zip[B](that: GenIterable[B]) = Empty
    def zipWithIndex = Empty
  }

}


trait ObjectMapping[A, B] {
  def apply(e:A) : B
}


/**
 *
 * @tparam A
 */
trait SilkIterable[+A] extends SilkIterableImpl[A, SilkIterable[A]] {
  def isEmpty : Boolean
  def isSingle : Boolean

  def size : Int
  def foreach[U](f: A => U) : Unit
  def map[B, That](f: A => B)(implicit bf:CanBuildFrom[A, B, That]) : That
  def flatMap[B, That](f: A => GenTraversableOnce[B])(implicit bf:CanBuildFrom[SilkIterable[A], B, That]) : That

  /**
   * Extract a projection B of A. This function is used to extract a sub set of
   * columns(parameters)
   * @tparam B target object
   * @return
   */
  def project[B, That](implicit mapping:ObjectMapping[A, B], bf:CanBuildFrom[SilkIterable[A], B, That]) : That

  def filter(pred: A => Boolean) : Silk[A]

  def collect[B, That](pf:PartialFunction[A, B])(implicit bf:CanBuildFrom[SilkIterable[A], B, That]) : That

  def reduce[A1 >: A](f: (A1, A1) => A1) : Silk[A1]
  def fold[A1 >: A](z:A1)(f: (A1, A1) => A1) : Silk[A1]

  def forall(pred: A => Boolean) : Boolean

  def iterator : Iterator[A]

  def zip[B, That](that:GenIterable[B])(implicit bf:CanBuildFrom[SilkIterable[A], (A, B), That]): That
  def zipWithIndex: Silk[(A, Int)]
}

trait SilkIterableImpl[+A, +Repr <: SilkIterable[A]]  {

  import scala.util.control.Breaks.{break, breakable}

  def repr = this.asInstanceOf[Repr]

  def newBuilder : mutable.Builder[A, Repr]

  def isEmpty : Boolean = {
    var hasElem = true
    for(e <- this) {
      breakable {
        hasElem = false
        break
      }
    }
    hasElem
  }

  def isSingle : Boolean = size == 1
  def size : Int = {
    var count = 0
    for(e <- this)
      count += 1
    count
  }

  def foreach[U](f: A => U) { for(x <- this) f(x) }

  def map[B, That](f: A => B)(implicit bf:CanBuildFrom[Repr, B, That]) : That = {
    val b = bf.apply()
    for(e <- this) b += f(e)
    b.result
  }
  def flatMap[B, That](f: A => GenTraversableOnce[B])(implicit bf:CanBuildFrom[Repr, B, That]) : That = {
    val b = bf.apply
    for(e <- this) b ++= f(e).seq
    b.result
  }

  /**
   * Extract a projection B of A. This function is used to extract a sub set of
   * columns(parameters)
   * @tparam B target object
   * @return
   */
  def project[B, That](implicit mapping:ObjectMapping[A, B], bf:CanBuildFrom[Repr, B, That]) : That = {
    val b = bf(repr)
    for(e <- this) b += mapping(e)
    b.result
  }

  def filter(pred: A => Boolean) : Repr = {
    val b = newBuilder
    for(x <- this)
      if(pred(x)) b += x
    b.result
  }

  def collect[B, That](pf:PartialFunction[A, B])(implicit bf:CanBuildFrom[Repr, B, That]) : That = {
    val b = bf(repr)
    for(e <- this)
      if(pf.isDefinedAt(e)) b += pf(e)
    b.result
  }

  def reduce[A1 >: A](f: (A1, A1) => A1) : Silk[A1]
  def fold[A1 >: A](z:A1)(f: (A1, A1) => A1) : Silk[A1]

  def forall(pred: A => Boolean) : Boolean

  def iterator : Iterator[A]

  def zip[B](that:GenIterable[B]): Silk[(A, B)]
  def zipWithIndex: Silk[(A, Int)]


}



/**
 *
 * @tparam A
 */
trait Silk[+A] extends SilkIterable[A] {

  def groupBy[K](keyExtractor: A => K) : Silk[(K, Silk[A])]
  def join[K, B](other:Silk[B], k1: A => K, k2: B => K) : Silk[(K, Silk[(A, B)])]
  def joinBy[B](other:Silk[B], cond: (A, B) => Boolean) : Silk[(A, B)]
  def sortBy[K](keyExtractor: A => K)(implicit ord:Ordering[K]) : Silk[A]
  def sortBy(ord: Ordering[A]) : Silk[A]
  def takeSample(proportion:Double) : Silk[A]

}


object InMemorySilk {
  def apply[A](s:Seq[A]) = new InMemorySilk(s)

  def newBuilder[A] : mutable.Builder[A, Silk[A]] = new InMemorySilkBuilder[A]

  class InMemorySilkBuilder[A] extends mutable.Builder[A, Silk[A]] {
    private val b = Seq.newBuilder[A]

    def +=(elem: A) = {
      b += elem
      this
    }
    def clear() { b.clear }

    def result() = new InMemorySilk[A](b.result)
  }


  class InMemorySilkCanBuildFrom[A, B, That[B]] extends CanBuildFrom[A, B, Silk[B]] {
    def apply(from: A) = newBuilder
    def apply() = newBuilder
  }

}

class InMemorySilk[A](elem:Seq[A]) extends Silk[A] {

  protected def newBuilder = InMemorySilk.newBuilder[A]


  def flatMap[B, That](f: (A) => GenTraversableOnce[B])(implicit bf:CanBuildFrom[A, B, That]) = {
    val b = bf.apply
    for(x <- elem) b ++= f(x).seq
    b.result
  }

  /**
   * Extract a projection B of A. This function is used to extract a sub set of
   * columns(parameters)
   * @tparam B target object
   * @return
   */
  def project[B, That](implicit mapping:ObjectMapping[A, B], bf:CanBuildFrom[A, B, That]) = {
    val b = bf.apply
    for(x <- elem) b += mapping(x)
    b.result
  }

  def filter(pred: (A) => Boolean) = {
    val b = newBuilder
    for(x <- elem)
      if(pred(x)) b += x
    b.result
  }

  def collect[B](pf: PartialFunction[A, B]) = null

  def reduce[A1 >: A](f: (A1, A1) => A1) = null

  def fold[A1 >: A](z: A1)(f: (A1, A1) => A1) = null

  def forall(pred: (A) => Boolean) = false

  def groupBy[K](keyExtractor: (A) => K) = null

  def join[K, B](other: Silk[B], k1: (A) => K, k2: (B) => K) = null

  def joinBy[B](other: Silk[B], cond: (A, B) => Boolean) = null

  def sortBy[K](keyExtractor: (A) => K)(implicit ord: Ordering[K]) = null

  def sortBy(ord: Ordering[A]) = null

  def takeSample(proportion: Double) = null

  def iterator = null

  def zip[B](that: GenIterable[B]) = null

  def zipWithIndex = null
}



