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
    def eval : To
  }

  trait SilkFlowBase[P, A, Repr <: Silk[A]] extends Silk[A]  {
    def iterator = null
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
    def sum[B >: A](implicit num: Numeric[B]) = Sum(this, num)
    def product[B >: A](implicit num: Numeric[B]) = Product(this, num)
    def min[B >: A](implicit cmp: Ordering[B]) = Min(this, cmp)
    def max[B >: A](implicit cmp: Ordering[B]) = Max(this, cmp)
    def maxBy[B](f: (A) => B)(implicit cmp: Ordering[B]) = MaxBy(this, f, cmp)
    def minBy[B](f: (A) => B)(implicit cmp: Ordering[B]) = MinBy(this, f, cmp)
    def mkString(start: String, sep: String, end: String) = null
    def groupBy[K](f: (A) => K) = null
    def project[B](implicit mapping: ObjectMapping[A, B]) = null
    def join[K, B](other: Silk[B], k1: (A) => K, k2: (B) => K) = null
    def joinBy[B](other: Silk[B], cond: (A, B) => Boolean) = null
    def sortBy[K](keyExtractor: (A) => K)(implicit ord: Ordering[K]) = null
    def sorted[A1 >: A](implicit ord: Ordering[A1]) = null
    def takeSample(proportion: Double) = null
    def withFilter(p: (A) => Boolean) = null
    def zip[B](other: Silk[B]) = null
    def zipWithIndex = null
  }

  trait SilkFlow[From, To] extends SilkFlowBase[From, To, Silk[To]] with Flow[From, Silk[To]]
  trait SilkFlowSingle[From, To] extends SilkSingle[To] with SilkFlowBase[From, To, SilkSingle[To]] with Flow[From, SilkSingle[To]]



  trait SilkFlowKnot[To] extends SilkFlowBase[Any, To, Silk[To]]

  case class Foreach[A, U](prev:Silk[A], f: A=> U) extends SilkFlow[A, U] {
    def eval : Silk[U] = prev.foreach(f)
  }

  case class Map[A, B](prev:Silk[A], f: A=>B) extends SilkFlow[A, B] {
    def eval : Silk[B] = {
      prev.map(f)
    }
  }

  case class FlatMap[A, B](prev:Silk[A], f: A=>GenTraversableOnce[B]) extends SilkFlow[A, B] {
    def eval : Silk[B] = {
      prev.flatMap(f)
    }
  }

  case class Filter[A](prev:Silk[A], f: A=>Boolean) extends SilkFlow[A, A] {
    def eval : Silk[A] = {
      prev.filter(f)
    }
  }

  case class Collect[A, B](prev:Silk[A], pf: PartialFunction[A, B]) extends SilkFlow[A, B] {
    def eval : Silk[B] = {
      prev.collect(pf)
    }
  }

  case class CollectFirst[A, B](prev:Silk[A], pf:PartialFunction[A, B]) extends SilkFlowSingle[A, Option[B]] {
    def eval = {
      prev.collectFirst(pf)
    }
  }

  case class Aggregate[A, B](prev:Silk[A], z: B, seqop: (B, A) => B, combop: (B, B) => B) extends SilkFlowSingle[A, B] {
    def eval = {
      prev.aggregate(z)(seqop, combop)
    }
  }

  case class Reduce[A, A1>:A](prev:Silk[A], op: (A1, A1) => A1) extends SilkFlowSingle[A, A1] {
    def eval = {
      prev.reduce(op)
    }
  }

  case class ReduceLeft[A, A1>:A](prev:Silk[A], op: (A1, A) => A1) extends SilkFlowSingle[A, A1] {
    def eval = {
      prev.reduceLeft(op)
    }
  }
  case class Fold[A, A1>:A](prev:Silk[A], z:A1, op: (A1, A1) => A1) extends SilkFlowSingle[A, A1] {
    def eval = {
      prev.fold(z)(op)
    }
  }

  case class FoldLeft[A, B](prev:Silk[A], z:B, op: (B, A) => B) extends SilkFlowSingle[A, B] {
    def eval = {
      prev.foldLeft(z)(op)
    }
  }

  case class Sum[A, B >: A](prev:Silk[A], m:Numeric[B]) extends SilkFlowSingle[A, B] {
    def eval = {
      prev.sum(m)
    }
  }

  case class Product[A, B >: A](prev:Silk[A], m:Numeric[B]) extends SilkFlowSingle[A, B] {
    def eval = {
      prev.product(m)
    }
  }

  case class Min[A, B >: A](prev:Silk[A], m:Ordering[B]) extends SilkFlowSingle[A, A] {
    def eval = {
      prev.min(m)
    }
  }

  case class Max[A, B >: A](prev:Silk[A], m:Ordering[B]) extends SilkFlowSingle[A, A] {
    def eval = {
      prev.max(m)
    }
  }


  case class MaxBy[A, B](prev:Silk[A], f: (A) => B, m:Ordering[B]) extends SilkFlowSingle[A, A] {
    def eval = {
      prev.maxBy(f)(m)
    }
  }

  case class MinBy[A, B](prev:Silk[A], f: (A) => B, m:Ordering[B]) extends SilkFlowSingle[A, A] {
    def eval = {
      prev.minBy(f)(m)
    }
  }




}