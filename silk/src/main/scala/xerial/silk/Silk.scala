//--------------------------------------
//
// Silk.scala
// Since: 2012/11/30 2:33 PM
//
//--------------------------------------

package xerial.silk

import java.io.{ByteArrayInputStream, ObjectInputStream, ByteArrayOutputStream, ObjectOutputStream}
import collection._
import generic.{TraversableFactory, GenericCompanion, GenericTraversableTemplate, CanBuildFrom}
import collection.mutable.{Builder, ArraySeq}
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

  object Empty extends SilkImplBase[Nothing] {
    def iterator = Iterator.empty
    protected[this] def newSilkBuilder = InMemorySilk.newBuilder
  }

}


trait ObjectMapping[-A, +B] {
  def apply(e:A) : B
}


/**
 * A trait for all Silk data types
 * @tparam A
 */
trait Silk[+A] extends GenSilk[A]  {

}

trait SilkImplBase[A] extends SilkLike[A, GenSilk[A]] with Silk[A]

/**
 * A common trait for implementing silk operations
 * @tparam A
 */
trait GenSilk[+A]
  extends SilkOps[A, GenSilk[A]]
  with GenTraversable[A]
  with GenIterable[A]
{

}

/**
 * A trait that defines silk specific operations
 * @tparam A
 * @tparam Repr
 */
trait SilkOps[+A, +Repr] {

  def isSingle : Boolean

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
  def sorted[A1 >: A](implicit ord: Ordering[A1]) : Repr

  def takeSample(proportion:Double) : Repr
}


/**
 * A basic implementation of [[xerial.silk.GenSilk]]
 * @tparam A
 * @tparam Repr
 */
trait SilkLike[+A, +Repr <: GenSilk[A]] extends SilkOps[A, Repr] with Iterable[A] with GenericTraversableTemplate[A, GenSilk] {

  def isSingle = size == 1

  //protected[this] def newSilkBuilder : Builder[A, Repr] = companion.newBuilder[A]

  def project[B, That](implicit mapping:ObjectMapping[A, B], bf:CanBuildFrom[Repr, B, That]) : That = {
    val b = bf.apply
    for(e <- this) b += mapping(e)
    b.result
  }

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
    for(a <- this; b <- other) {
      if(cond(a, b))
        m += ((a, b))
    }
    m.result
  }

  def sortBy[K](keyExtractor: A => K)(implicit ord:Ordering[K]) : Repr = sorted(ord on keyExtractor)
  def sorted[A1 >: A](implicit ord: Ordering[A1]) : Repr = {
    val len = this.length
    val arr = toArraySeq
    java.util.Arrays.sort(arr.array, ord.asInstanceOf[Ordering[Object]])
    val b = newBuilder
    b.sizeHint(len)
    for (x <- arr) b += x
    b.result.asInstanceOf[Repr]
  }

  private def toArraySeq[A1 >: A] : ArraySeq[A1] = {
    val arr = new ArraySeq[A1](this.length)
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
    b.result.asInstanceOf[Repr]
  }

}





object InMemorySilk extends TraversableFactory[InMemorySilk] {
  def apply[A](s:Seq[A]) = new InMemorySilk(s)

  def newBuilder[A] : mutable.Builder[A, InMemorySilk[A]] = new InMemorySilkBuilder[A]

  class InMemorySilkBuilder[A] extends mutable.Builder[A, InMemorySilk[A]] {
    private val b = Seq.newBuilder[A]

    def +=(elem: A) = {
      b += elem
      this
    }
    def clear() { b.clear }

    def result() = new InMemorySilk[A](b.result)
  }


  class InMemorySilkCanBuildFrom[A, B, That[B]] extends CanBuildFrom[A, B, InMemorySilk[B]] {
    def apply(from: A) = newBuilder
    def apply() = newBuilder
  }

}

class InMemorySilk[A](elem:Seq[A]) extends Silk[A] with SilkLike[A, InMemorySilk[A]] {
  def iterator = elem.iterator
  override def companion: GenericCompanion[InMemorySilk] = InMemorySilk
}



