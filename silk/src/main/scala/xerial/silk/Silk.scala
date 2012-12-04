//--------------------------------------
//
// Silk.scala
// Since: 2012/11/30 2:33 PM
//
//--------------------------------------

package xerial.silk

import java.io.{ByteArrayInputStream, ObjectInputStream, ByteArrayOutputStream, ObjectOutputStream}
import collection._
import generic._
import collection.mutable.{Builder, ArraySeq}
import scala.Iterator
import scala.Seq
import scala.Iterable
import scala.Ordering
import scala.util.Random
import scala.Some

/**
 * @author Taro L. Saito
 */
object Silk {

  def toSilk[A](obj:A) : Silk[A] = {
    new InMemorySilk[A](Seq(obj))
  }

  object Empty extends Silk[Nothing] with SilkLike[Nothing] {
    def newBuilderOf[T] = InMemorySilk.newBuilder[T]
    def iterator = Iterator.empty
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


/**
 * A common trait for implementing silk operations
 * @tparam A
 */
trait GenSilk[+A]
  extends SilkOps[A]
{

}



/**
 * A trait that defines silk specific operations
 * @tparam A
 */
trait SilkOps[+A] {

  def iterator : Iterator[A]

  def newBuilderOf[T] : Builder[T, Silk[T]]

  def foreach[U](f: A => U)
  def map[B](f: A => B) : Silk[B]
  def flatMap[B](f: A => GenTraversable[B]) : Silk[B]

  def filter(p: A => Boolean) : Silk[A]
  def filterNot(p: A => Boolean) : Silk[A] = filter({ x => !p(x) })

  def collect[B](pf:PartialFunction[A, B]) : Silk[B]
  def collectFirst[B](pf:PartialFunction[A, B]) : Option[B]

  def aggregate[B](z:B)(seqop:(B, A) => B, combop:(B, B)=>B):B
  def reduce[A1 >: A](op: (A1, A1) => A1): A1
  def reduceLeft[B >: A](op:(B, A) => B) : B
  def fold[A1 >: A](z:A1)(op: (A1, A1) => A1): A1
  def foldLeft[B](z:B)(op:(B, A) => B): B


  def size: Int
  def isSingle : Boolean
  def isEmpty : Boolean

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
  def project[B](implicit mapping:ObjectMapping[A, B]) : Silk[B]
  def join[K, B](other:Silk[B], k1: A => K, k2: B => K) : Silk[(K, Silk[(A, B)])]
  def joinBy[B](other:Silk[B], cond: (A, B) => Boolean) : Silk[(A, B)]
  def sortBy[K](keyExtractor: A => K)(implicit ord:Ordering[K]) : Silk[A]
  def sorted[A1 >: A](implicit ord: Ordering[A1]) : Silk[A1]

  def takeSample(proportion:Double) : Silk[A]


  def withFilter(p: A => Boolean): SilkMonadicFilter[A]
}

/**
 * A trait for supporting for(x <- Silk[A] if cond) syntax
 * @tparam A
 */
trait SilkMonadicFilter[+A] {
  def map[B](f: A => B) : Silk[B]
  def flatMap[B](f: A => collection.GenTraversableOnce[B]): Silk[B]
  def foreach[U](f: A => U): Unit
  def withFilter(p: A => Boolean): SilkMonadicFilter[A]
}

/**
 * A basic implementation of [[xerial.silk.GenSilk]]
 * @tparam A
 */
trait SilkLike[+A] extends SilkOps[A] {

  def isEmpty = iterator.isEmpty

  def aggregate[B](z:B)(seqop:(B, A) => B, combop:(B, B)=>B):B = foldLeft(z)(seqop)

  def reduce[A1 >: A](op: (A1, A1) => A1): A1 = reduceLeft(op)

  def reduceLeft[B >: A](op: (B, A) => B): B = {
    var first = true
    var acc: B = 0.asInstanceOf[B]

    for (x <- iterator) {
      if (first) {
        acc = x
        first = false
      }
      else acc = op(acc, x)
    }
    acc
  }

  def foldLeft[B](z:B)(op:(B, A) => B): B = {
    var result = z
    foreach (x => result = op(result, x))
    result
  }

  def fold[A1 >: A](z:A1)(op: (A1, A1) => A1): A1 = foldLeft(z)(op)


  def sum[B >: A](implicit num: Numeric[B]): B = foldLeft(num.zero)(num.plus)

  def product[B >: A](implicit num: Numeric[B]): B = foldLeft(num.one)(num.times)

  def min[B >: A](implicit cmp: Ordering[B]): A = {
    if (isEmpty)
      throw new UnsupportedOperationException("empty.min")
    reduceLeft((x, y) => if (cmp.lteq(x, y)) x else y)
  }

  def max[B >: A](implicit cmp: Ordering[B]): A = {
    if (isEmpty)
      throw new UnsupportedOperationException("empty.max")

    reduceLeft((x, y) => if (cmp.gteq(x, y)) x else y)
  }

  def maxBy[B](f: A => B)(implicit cmp: Ordering[B]): A = {
    if (isEmpty)
      throw new UnsupportedOperationException("empty.maxBy")

    reduceLeft((x, y) => if (cmp.gteq(f(x), f(y))) x else y)
  }
  def minBy[B](f: A => B)(implicit cmp: Ordering[B]): A = {
    if (isEmpty)
      throw new UnsupportedOperationException("empty.minBy")

    reduceLeft((x, y) => if (cmp.lteq(f(x), f(y))) x else y)
  }


  def mkString(start: String, sep: String, end: String): String =
    addString(new StringBuilder(), start, sep, end).toString

  def addString(b: StringBuilder, start: String, sep: String, end: String): StringBuilder = {
    var first = true

    b append start
    for (x <- iterator) {
      if (first) {
        b append x
        first = false
      }
      else {
        b append sep
        b append x
      }
    }
    b append end

    b
  }


  def size = {
    var count = 0
    for(x <- this.iterator)
      count += 1
    count
  }


  def foreach[U](f: A => U) {
    for(x <- this.iterator)
      f(x)
  }

  def map[B](f: A => B) : Silk[B] = {
    val b = newBuilderOf[B]
    for(x <- this.iterator)
      b += f(x)
    b.result
  }

  def flatMap[B](f: A => GenTraversable[B]) : Silk[B] = {
    val b = newBuilderOf[B]
    for(x <- this.iterator; s <- f(x)) {
      b += s
    }
    b.result
  }

  def filter(p: A => Boolean) : Silk[A] = {
    val b = newBuilderOf[A]
    for(x <- this.iterator if p(x))
      b += x
    b.result
  }


  def collect[B](pf:PartialFunction[A, B]) : Silk[B] = {
    val b = newBuilderOf[B]
    for(x <- this.iterator; if(pf.isDefinedAt(x))) b += pf(x)
    b.result
  }

  def collectFirst[B](pf: PartialFunction[A, B]): Option[B] = {
    for (x <- iterator) { // make sure to use an iterator or `seq`
      if (pf isDefinedAt x)
        return Some(pf(x))
    }
    None
  }


  def isSingle = size == 1

  def project[B](implicit mapping:ObjectMapping[A, B]) : Silk[B] = {
    val b = newBuilderOf[B]
    for(e <- this) b += mapping(e)
    b.result
  }

  def length: Int = size


  def groupBy[K](f: A => K): Silk[(K, Silk[A])] = {
    val m = mutable.Map.empty[K, Builder[A, Silk[A]]]
    for(elem <- iterator) {
      val key = f(elem)
      val b = m.getOrElseUpdate(key, newBuilderOf[A])
      b += elem
    }
    val r = newBuilderOf[(K, Silk[A])]
    for((k, b) <- m) {
      r += k -> b.result
    }
    r.result
  }

  def join[K, B](other:Silk[B], k1: A => K, k2: B => K): Silk[(K, Silk[(A, B)])] = {

    def createMap[T](lst:SilkOps[T], f: T => K): Map[K, Builder[T, Seq[T]]] = {
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

    val m = newBuilderOf[(K, Silk[(A, B)])]
    for(k <- a.keys) yield {
      val pairs = newBuilderOf[(A, B)]
      for(ae <- a(k).result; be <-b(k).result) {
        pairs += ((ae, be))
      }
      m += k -> pairs.result()
    }
    m.result
  }

  def joinBy[B](other:Silk[B], cond: (A, B) => Boolean) : Silk[(A, B)] = {
    val m = newBuilderOf[(A, B)]
    for(a <- this; b <- other) {
      if(cond(a, b))
        m += ((a, b))
    }
    m.result
  }

  def sortBy[K](keyExtractor: A => K)(implicit ord:Ordering[K])  = sorted(ord on keyExtractor)
  def sorted[A1 >: A](implicit ord: Ordering[A1]) : Silk[A1] = {
    val len = this.length
    val arr = toArraySeq
    java.util.Arrays.sort(arr.array, ord.asInstanceOf[Ordering[Object]])
    val b = newBuilderOf[A1]
    b.sizeHint(len)
    for (x <- arr) b += x
    b.result
  }

  private def toArraySeq[A1 >: A] : ArraySeq[A1] = {
    val arr = new ArraySeq[A1](this.length)
    var i = 0
    for (x <- this) {
      arr(i) = x
      i += 1
    }
    arr
  }

  def takeSample(proportion:Double) : Silk[A] = {
    val arr = toArraySeq
    val N = arr.length
    val num = (N * proportion + 0.5).toInt
    var i = 0
    val b = newBuilderOf[A]
    (0 until num).foreach { i =>
      b += arr(Random.nextInt(N))
    }
    b.result
  }


  def withFilter(p: A => Boolean) = new WithFilter(p)

  class WithFilter(p: A => Boolean) extends SilkMonadicFilter[A] {

    def map[B](f: (A) => B) : Silk[B] = {
      val b = newBuilderOf[B]
      for(x <- iterator)
        if(p(x)) b += f(x)
      b.result
    }

    def flatMap[B](f: (A) => GenTraversableOnce[B]) : Silk[B] = {
      val b = newBuilderOf[B]
      for(x <- iterator; e <- f(x))
        if(p(x)) b += e
      b.result
    }

    def foreach[U](f: (A) => U) {
      for(x <- iterator)
        if(p(x)) f(x)
    }

    def withFilter(q: (A) => Boolean) : WithFilter = new WithFilter(x => p(x) && q(x))
  }


}





object InMemorySilk {
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


  class InMemorySilkCanBuildFrom[A, B, That] extends CanBuildFrom[A, B, Silk[B]] {
    def apply(from: A) = newBuilder
    def apply() = newBuilder
  }

}

class InMemorySilk[A](elem:Seq[A]) extends Silk[A] with SilkLike[A] {
  def iterator = elem.iterator

  def newBuilderOf[T] = InMemorySilk.newBuilder[T]

}



