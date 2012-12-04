//--------------------------------------
//
// Silk.scala
// Since: 2012/11/30 2:33 PM
//
//--------------------------------------

package xerial.silk

import java.io.{ByteArrayInputStream, ObjectInputStream, ByteArrayOutputStream, ObjectOutputStream}
import collection._
import generic.{GenericTraversableTemplate, CanBuildFrom}
import mutable.{Builder, ArraySeq}
import scala.Iterator
import scala.Seq
import scala.Iterable
import scala.Ordering
import scala.util.Random

/**
 * @author Taro L. Saito
 */
object Silk {

  def toSilk[A](obj:A) : Silk[A] = {
    new InMemorySilk[A](Seq(obj))
  }

  object Empty extends SilkIterable[Nothing] {
    def iterator = Iterator.empty
  }

}


trait ObjectMapping[A, B] {
  def apply(e:A) : B
}


/**
 *
 * @tparam A
 */
trait SilkIterable[+A] extends SilkIterableImpl[A, SilkGenIterable[A]]
{

}


trait SilkGenIterable[+A]
  extends SilkGenIterableLike[A, SilkGenIterable[A]]
  with GenTraversable[A]
  with GenIterable[A]
{
  def isSingle : Boolean = size == 1
  def aggregate[B](z:B)(seqop:(B, A) => B, combop: (B, B) => B): B

}

trait SilkGenIterableLike[+A, +Repr] {

  /**
   * Extract a projection B of A. This function is used to extract a sub set of
   * columns(parameters)
   * @tparam B target object
   * @return
   */
  def project[B, That](implicit mapping:ObjectMapping[A, B], bf:CanBuildFrom[Repr, B, That]) : That
  def join[K, B, That](other:GenIterable[B], k1: A => K, k2: B => K)(implicit bf:CanBuildFrom[Repr, (A, B), That]) : Map[K, That]
  def joinBy[B, That](other:GenIterable[B], cond: (A, B) => Boolean)(implicit bf:CanBuildFrom[Repr, (A, B), That]) : That
  def sortBy[K](keyExtractor: A => K)(implicit ord:Ordering[K]) : Repr
  def sortBy(implicit ord: Ordering[A]) : Repr

  def takeSample(proportion:Double) : Repr
}

trait SilkIterableImpl[+A, +Repr <: GenIterable[A]] extends SilkGenIterableLike[A, Repr] with Iterable[A] {

  //override def repr = this.asInstanceOf[Repr]

  /**
   * Extract a projection B of A. This function is used to extract a sub set of
   * columns(parameters)
   * @tparam B target object
   * @return
   */
  def project[B, That](implicit mapping:ObjectMapping[A, B], bf:CanBuildFrom[Repr, B, That]) : That = {
    val b = bf.apply
    for(e <- this) b += mapping(e)
    b.result
  }

  protected[this] def newBuilder: Builder[A, Repr]

  def length: Int = size

  def join[K, B, That](other:GenIterable[B], k1: A => K, k2: B => K)(implicit bf:CanBuildFrom[Repr, (A, B), That]) : Map[K, That] = {

    def createMap[T](lst:GenIterable[T], f: T => K): Map[K, Builder[T, Seq[T]]] = {
      val m = mutable.Map.empty[K, Builder[T, Seq[T]]]
      for(elem <- lst) {
        val key = f(elem)
        val b = m.getOrElseUpdate(key, Seq.newBuilder[T])
        b += elem
      }
      m
    }
    val a : Map[K, Builder[A, Seq[A]]] = createMap(this, k1)
    val b : Map[K, Builder[B, Seq[B]]] = createMap(other, k2)

    val m = Map.newBuilder[K, That]
    for(k <- a.keys) yield {
      val pairs = bf.apply
      for(ae <- a(k).result; be <-b(k).result) {
        pairs += ((ae, be))
      }
      m += k -> pairs.result()
    }
    m.result
  }

  def joinBy[B, That](other:GenIterable[B], cond: (A, B) => Boolean)(implicit bf:CanBuildFrom[Repr, (A, B), That]) : That = {
    val m = bf.apply
    for(a <- this; b <- other if cond(a, b)) {
      m += ((a, b))
    }
    m.result
  }

  def sortBy[K](keyExtractor: A => K)(implicit ord:Ordering[K]) : Repr = sortBy(ord on keyExtractor)
  def sortBy(implicit ord: Ordering[A]) : Repr = {
    val len = this.length
    val arr = toArraySeq
    java.util.Arrays.sort(arr.array, ord.asInstanceOf[Ordering[Object]])
    val b = newBuilder
    b.sizeHint(len)
    for (x <- arr) b += x
    b.result
  }

  private def toArraySeq : ArraySeq[A] = {
    val arr = new ArraySeq[A](this.length)
    var i = 0
    for (x <- this.seq) {
      arr(i) = x
      i += 1
    }
    arr
  }

  def takeSample(proportion:Double) : Repr = {
    val arr = toArraySeq
    val N = arr.length
    val num = (N * proportion + 0.5).toInt
    var i = 0
    val b = newBuilder
    (0 until num).foreach { i =>
      b += arr(Random.nextInt(N))
    }
    b.result
  }

}





/**
 *
 * @tparam A
 */
trait Silk[+A] extends SilkGenIterable[A]  {

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

class InMemorySilk[A](elem:Seq[A]) extends Silk[A] with SilkIterable[A] {
  def iterator = elem.iterator
}



