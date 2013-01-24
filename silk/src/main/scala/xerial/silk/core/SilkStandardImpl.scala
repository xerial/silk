//--------------------------------------
//
// SilkStandardImpl.scalapl.scala
// Since: 2012/12/04 4:24 PM
//
//--------------------------------------

package xerial.silk.core

import collection.{TraversableOnce, GenTraversableOnce, mutable, GenTraversable}
import util.Random
import reflect.ClassTag

/**
 * A standard implementation of [[xerial.silk.core.Silk]]
 * @tparam A
 */
trait SilkStandardImpl[+A] extends SilkOps[A] { self =>

  def foreach[U](f: A => U) = {
    for(x <- this.iterator)
      f(x)
    val b = newBuilder[U]
    b.result
  }

  def map[B](f: A => B) : Silk[B] = {
    val b = newBuilder[B]
    for(x <- this)
      b += f(x)
    b.result
  }

  def flatMap[B](f: A => GenTraversableOnce[B]) : Silk[B] = {
    val b = newBuilder[B]
    for(x <- this; s <- f(x)) {
      b += s
    }
    b.result
  }

  def filter(p: A => Boolean) : Silk[A] = {
    val b = newBuilder[A]
    for(x <- this if p(x))
      b += x
    b.result
  }


  def collect[B](pf:PartialFunction[A, B]) : Silk[B] = {
    val b = newBuilder[B]
    for(x <- this; if(pf.isDefinedAt(x))) b += pf(x)
    b.result
  }

  def collectFirst[B](pf: PartialFunction[A, B]): SilkSingle[Option[B]] = {
    for (x <- this) { // make sure to use an iterator or `seq`
      if (pf isDefinedAt x) {
        return Silk.single(Some(pf(x)))
      }
    }
    return Silk.single(None)
  }


  def isEmpty = iterator.isEmpty

  def aggregate[B](z:B)(seqop:(B, A) => B, combop:(B, B)=>B): SilkSingle[B] = foldLeft(z)(seqop)

  def reduce[A1 >: A](op: (A1, A1) => A1): SilkSingle[A1] = reduceLeft(op)

  def reduceLeft[B >: A](op: (B, A) => B): SilkSingle[B] = {
    var first = true
    var acc: B = 0.asInstanceOf[B]

    for (x <- iterator) {
      if (first) {
        acc = x
        first = false
      }
      else acc = op(acc, x)
    }

    Silk.single(acc)
  }

  def foldLeft[B](z:B)(op:(B, A) => B): SilkSingle[B] = {
    var result = z
    foreach (x => result = op(result, x))
    Silk.single(result)
  }

  def fold[A1 >: A](z:A1)(op: (A1, A1) => A1): SilkSingle[A1] = foldLeft(z)(op)


  def scanLeftWith[B, C](z: B)(op : (B, A) => (B, C)): Silk[C] = {
    val b = newBuilder[C]
    var zi = z
    for(x <- this) {
      val (zn, c) = op(zi, x)
      b += c
      zi = zn
    }
    b.result
  }


  def split : Silk[Silk[A]] = {
    val seq = newBuilder[Silk[A]]
    val b = newBuilder[A]
    val splitSize = 1000 // TODO externalize split size
    var count = 0
    for(x <- this) {
      if(count < splitSize) {
        b += x
        count += 1
      }
      else {
        seq += b.result
        count = 0
      }
    }

    val r = b.result
    if(!r.isEmpty)
      seq += r

    seq.result
  }

  def sum[B >: A](implicit num: Numeric[B]): SilkSingle[B] = foldLeft(num.zero)(num.plus)

  def product[B >: A](implicit num: Numeric[B]): SilkSingle[B] = foldLeft(num.one)(num.times)

  def min[B >: A](implicit cmp: Ordering[B]): SilkSingle[A] = {
    if (isEmpty)
      throw new UnsupportedOperationException("empty.min")
    reduceLeft((x, y) => if (cmp.lteq(x, y)) x else y)
  }

  def max[B >: A](implicit cmp: Ordering[B]): SilkSingle[A] = {
    if (isEmpty)
      throw new UnsupportedOperationException("empty.max")

    reduceLeft((x, y) => if (cmp.gteq(x, y)) x else y)
  }

  def maxBy[B](f: A => B)(implicit cmp: Ordering[B]): SilkSingle[A] = {
    if (isEmpty)
      throw new UnsupportedOperationException("empty.maxBy")

    reduceLeft((x, y) => if (cmp.gteq(f(x), f(y))) x else y)
  }
  def minBy[B](f: A => B)(implicit cmp: Ordering[B]): SilkSingle[A] = {
    if (isEmpty)
      throw new UnsupportedOperationException("empty.minBy")

    reduceLeft((x, y) => if (cmp.lteq(f(x), f(y))) x else y)
  }


  def mkString(start: String, sep: String, end: String): SilkSingle[String] =
    Silk.single(addString(new StringBuilder(), start, sep, end).toString)

  def addString(b: StringBuilder, start: String, sep: String, end: String): StringBuilder = {
    var first = true

    b append start
    for (x <- this) {
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




  def isSingle = size == 1

  def project[B](implicit mapping:ObjectMapping[A, B]) : Silk[B] = {
    val b = newBuilder[B]
    for(e <- this) b += mapping(e)
    b.result
  }

  def length: Int = size


  def groupBy[K](f: A => K): Silk[(K, Silk[A])] = {
    val m = collection.mutable.Map.empty[K, mutable.Builder[A, Silk[A]]]
    for(elem <- iterator) {
      val key = f(elem)
      val b = m.getOrElseUpdate(key, newBuilder[A])
      b += elem
    }
    val r = newBuilder[(K, Silk[A])]
    for((k, b) <- m) {
      r += k -> b.result
    }
    r.result
  }

  def join[K, B](other:Silk[B], k1: A => K, k2: B => K): Silk[(K, Silk[(A, B)])] = {

    def createMap[T](lst:SilkOps[T], f: T => K): collection.mutable.Map[K, mutable.Builder[T, Seq[T]]] = {
      val m = collection.mutable.Map.empty[K, mutable.Builder[T, Seq[T]]]
      for(elem <- lst) {
        val key = f(elem)
        val b = m.getOrElseUpdate(key, Seq.newBuilder[T])
        b += elem
      }
      m
    }

    val a = createMap(this, k1)
    val b = createMap(other, k2)

    val m = newBuilder[(K, Silk[(A, B)])]
    for(k <- a.keys) yield {
      val pairs = newBuilder[(A, B)]
      for(ae <- a(k).result; be <-b(k).result) {
        pairs += ((ae, be))
      }
      m += k -> pairs.result()
    }
    m.result
  }

  def joinBy[B](other:Silk[B], cond: (A, B) => Boolean) : Silk[(A, B)] = {
    val m = newBuilder[(A, B)]
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
    val b = newBuilder[A1]
    b.sizeHint(len)
    for (x <- arr) b += x
    b.result
  }

  private def toArraySeq[A1 >: A] : mutable.ArraySeq[A1] = {
    val arr = new mutable.ArraySeq[A1](this.length)
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
    val b = newBuilder[A]
    (0 until num).foreach { i =>
      b += arr(Random.nextInt(N))
    }
    b.result
  }


  def withFilter(p: A => Boolean) = new WithFilter(p)

  def concat[B](implicit asTraversable: A => Silk[B]): Silk[B] = {
    val b = newBuilder[B]
    for (xs <- this; x <- asTraversable(xs))
      b += x
    b.result
  }

  class WithFilter(p: A => Boolean) extends SilkMonadicFilter[A] {

    def iterator = self.iterator.withFilter(p)
    def newBuilder[T] = self.newBuilder[T]

    override def foreach[U](f: (A) => U) = {
      for(x <- iterator) f(x)
      Silk.Empty
    }

    override def withFilter(q: (A) => Boolean) : WithFilter = new WithFilter(x => p(x) && q(x))
    def eval = this
  }

  def zip[B](other: Silk[B]) : Silk[(A, B)] = {
    val b = newBuilder[(A, B)]
    val ai = this.iterator
    val bi = other.iterator
    while(ai.hasNext && bi.hasNext)
      b += ((ai.next, bi.next))
    b.result
  }
  def zipWithIndex : Silk[(A, Int)] = {
    val b = newBuilder[(A, Int)]
    var i = 0
    for(x <- this) {
      b += ((x, i))
      i += 1
    }
    b.result
  }


  def toArray[B >: A : ClassTag] : Array[B] = iterator.toArray


  // TODO impl
  def save[B>:A] : Silk[B] = null
}
