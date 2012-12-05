//--------------------------------------
//
// SilkWorkflow.scala
// Since: 2012/12/04 4:49 PM
//
//--------------------------------------

package xerial.silk.collection

import collection.GenTraversableOnce

object Flow {


  trait Flow[From, +To] {
    def eval: To
  }

  trait SilkFlowBase[P, A, Repr <: Silk[A]] extends Silk[A] {

    // TODO impl
    def iterator = null
    // TODO impl
    def newBuilder[T] = null

    def foreach[U](f: (A) => U) = Foreach(this, f)
    def map[B](f: (A) => B) = Map(this, f)
    def flatMap[B](f: (A) => GenTraversableOnce[B]) = FlatMap(this, f)
    def filter(p: (A) => Boolean) = Filter(this, p)
    def collect[B](pf: PartialFunction[A, B]) = Collect(this, pf)
    def collectFirst[B](pf: PartialFunction[A, B]) = CollectFirst(this, pf)
    def aggregate[B](z: B)(seqop: (B, A) => B, combop: (B, B) => B) = Aggregate(this, z, seqop, combop)
    def reduce[A1 >: A](op: (A1, A1) => A1) = Reduce(this, op)
    def reduceLeft[B >: A](op: (B, A) => B) = ReduceLeft(this, op)
    def fold[A1 >: A](z: A1)(op: (A1, A1) => A1) = Fold(this, z, op)
    def foldLeft[B](z: B)(op: (B, A) => B) = FoldLeft(this, z, op)
    def size = 0
    def isSingle = false
    def isEmpty = false
    def sum[B >: A](implicit num: Numeric[B]) = Fold(this, num.zero, num.plus)
    def product[B >: A](implicit num: Numeric[B]) = Fold(this, num.one, num.times)
    def min[B >: A](implicit cmp: Ordering[B]) = Reduce(this, (x:A, y:A) => if (cmp.lteq(x, y)) x else y)
    def max[B >: A](implicit cmp: Ordering[B]) = Reduce(this, (x:A, y:A) => if (cmp.gteq(x, y)) x else y)
    def maxBy[B](f: (A) => B)(implicit cmp: Ordering[B]) = Reduce(this, (x:A, y:A) => if (cmp.gteq(f(x), f(y))) x else y)
    def minBy[B](f: (A) => B)(implicit cmp: Ordering[B]) = Reduce(this, (x:A, y:A) => if (cmp.lteq(f(x), f(y))) x else y)
    def mkString(start: String, sep: String, end: String) =
      Map[A, String](this, _.toString).aggregate(new StringBuilder)(
      {
        (b, a) =>
          if (!b.isEmpty)
            b.append(sep)
          b.append(a)
          b
      }, {
        (b1, b2) =>
          if (!b1.isEmpty)
            b1.append(sep)
          b1.append(b2.result())
          b1
      }).mapSingle {
        s =>
          val b = new StringBuilder
          b.append(start)
          b.append(s)
          b.append(end)
          b.result
      }

    def groupBy[K](f: (A) => K) = GroupBy(this, f)
    def project[B](implicit mapping: ObjectMapping[A, B]) = Project(this, mapping)
    def join[K, B](other: Silk[B], k1: (A) => K, k2: (B) => K) = Join(this, other, k1, k2)
    def joinBy[B](other: Silk[B], cond: (A, B) => Boolean) = JoinBy(this, other, cond)
    def sortBy[K](f: (A) => K)(implicit ord: Ordering[K]) = SortBy(this, f, ord)
    def sorted[A1 >: A](implicit ord: Ordering[A1]) = Sort(this, ord)
    def takeSample(proportion: Double) = Sampling(this, proportion)
    def withFilter(p: (A) => Boolean) = WithFilter(this, p)
    def zip[B](other: Silk[B]) = Zip(this, other)
    def zipWithIndex = ZipWithIndex(this)
  }

  trait SilkFlow[From, To] extends SilkFlowBase[From, To, Silk[To]] with Flow[From, Silk[To]]
  trait SilkFlowSingle[From, To] extends SilkSingle[To] with SilkFlowBase[From, To, SilkSingle[To]] with Flow[From, SilkSingle[To]] {
    def mapSingle[B](f: To => B) : SilkSingle[B] = eval.mapSingle(f)
  }

  trait SilkFilter[A] extends SilkMonadicFilter[A] with Flow[A, SilkMonadicFilter[A]] {
    // TODO impl
    def iterator = null
    def newBuilder[T] = null
  }

   case class Foreach[A, U](prev: Silk[A], f: A => U) extends SilkFlow[A, U] {
    def eval: Silk[U] = prev.foreach(f)
  }

  case class Map[A, B](prev: Silk[A], f: A => B) extends SilkFlow[A, B] {
    def eval: Silk[B] = {
      prev.map(f)
    }
  }

  case class FlatMap[A, B](prev: Silk[A], f: A => GenTraversableOnce[B]) extends SilkFlow[A, B] {
    def eval: Silk[B] = {
      prev.flatMap(f)
    }
  }

  case class Filter[A](prev: Silk[A], f: A => Boolean) extends SilkFlow[A, A] {
    def eval: Silk[A] = {
      prev.filter(f)
    }
  }

  case class Collect[A, B](prev: Silk[A], pf: PartialFunction[A, B]) extends SilkFlow[A, B] {
    def eval: Silk[B] = {
      prev.collect(pf)
    }
  }

  case class CollectFirst[A, B](prev: Silk[A], pf: PartialFunction[A, B]) extends SilkFlowSingle[A, Option[B]] {
    def eval = {
      prev.collectFirst(pf)
    }
  }

  case class Aggregate[A, B](prev: Silk[A], z: B, seqop: (B, A) => B, combop: (B, B) => B) extends SilkFlowSingle[A, B] {
    def eval = {
      prev.aggregate(z)(seqop, combop)
    }
  }

  case class Reduce[A, A1 >: A](prev: Silk[A], op: (A1, A1) => A1) extends SilkFlowSingle[A, A1] {
    def eval = {
      prev.reduce(op)
    }
  }

  case class ReduceLeft[A, A1 >: A](prev: Silk[A], op: (A1, A) => A1) extends SilkFlowSingle[A, A1] {
    def eval = {
      prev.reduceLeft(op)
    }
  }
  case class Fold[A, A1 >: A](prev: Silk[A], z: A1, op: (A1, A1) => A1) extends SilkFlowSingle[A, A1] {
    def eval = {
      prev.fold(z)(op)
    }
  }

  case class FoldLeft[A, B](prev: Silk[A], z: B, op: (B, A) => B) extends SilkFlowSingle[A, B] {
    def eval = {
      prev.foldLeft(z)(op)
    }
  }

  case class GroupBy[A, K](prev: Silk[A], f: A => K) extends SilkFlow[A, (K, Silk[A])] {
    def eval = {
      prev.groupBy[K](f)
    }
  }

  case class Project[A, B](prev: Silk[A], mapping: ObjectMapping[A, B]) extends SilkFlow[A, B] {
    def eval = prev.project(mapping)
  }

  case class Join[A, B, K](left: Silk[A], right: Silk[B], k1: (A) => K, k2: (B) => K) extends SilkFlow[(A, B), (K, Silk[(A, B)])] {
    def eval = left.join(right, k1, k2)
  }

  case class JoinBy[A, B](left: Silk[A], right: Silk[B], cond: (A, B) => Boolean) extends SilkFlow[(A, B), (A, B)] {
    def eval = left.joinBy(right, cond)
  }

  case class SortBy[A, K](prev: Silk[A], f: A => K, ord: Ordering[K]) extends SilkFlow[A, A] {
    def eval = prev.sortBy(f)(ord)
  }

  case class Sort[A, A1 >: A, K](prev: Silk[A], ord: Ordering[A1]) extends SilkFlow[A, A1] {
    def eval = prev.sorted(ord)
  }

  case class Sampling[A](prev: Silk[A], proportion: Double) extends SilkFlow[A, A] {
    def eval = prev.takeSample(proportion)
  }

  case class WithFilter[A](prev: Silk[A], p: A => Boolean) extends SilkFilter[A] {
    def eval = prev.withFilter(p)
  }

  case class Zip[A, B](prev: Silk[A], other: Silk[B]) extends SilkFlow[A, (A, B)] {
    def eval = prev.zip(other)
  }

  case class ZipWithIndex[A](prev: Silk[A]) extends SilkFlow[A, (A, Int)] {
    def eval = prev.zipWithIndex
  }

}