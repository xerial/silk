//--------------------------------------
//
// SilkSeq.scala
// Since: 2013/11/06 17:47
//
//--------------------------------------

package xerial.silk

import xerial.silk.core.SilkMacros
import scala.collection.GenTraversable
import xerial.silk.SilkException._
import scala.reflect.ClassTag
import scala.language.experimental.macros
import xerial.silk.core.Partitioner

/**
 * SilkSeq represents a sequence of elements. Silk data type contains FContext, class and variable names where
 * this SilkSeq is defined. In order to retrieve FContext information,
 * operators in Silk use Scala macros to inspect the AST of the program code.
 *
 * <h3>Implementaion note</h3>
 * You may consider moving all methods in SilkSeq to the parent Silk class, but Scala macro cannot be used
 * to implement methods declared in the super class. So all definitions are described here.
 *
 * Since methods defined using macros cannot be called within the same
 * class, each method in Silk must have a separate macro statement. That means you cannot reuse
 * the SilkSeq methods to implment other method (e.g. mkString(...))
 *
 */
abstract class SilkSeq[+A] extends Silk[A] {

  import SilkMacros._

  def isSingle = false
  def isEmpty(implicit env:SilkEnv) : Boolean = macro mIsEmpty[A]
  def size : SilkSingle[Long] = macro mSize[A]

  // Map with resources
  def mapWith[A, B, R1](r1:Silk[R1])(f: (A, R1) => B) : SilkSeq[B] = macro mMapWith[A, B, R1]
  def mapWith[A, B, R1, R2](r1:Silk[R1], r2:Silk[R2])(f:(A, R1, R2) => B) : SilkSeq[B] = macro mMap2With[A, B, R1, R2]

  // FlatMap with resources
  def flatMapWith[A, B, R1](r1:Silk[R1])(f:(A, R1) => Silk[B]) : SilkSeq[B] = macro mFlatMapWith[A, B, R1]
  def flatMapWith[A, B, R1, R2](r1:Silk[R1], r2:Silk[R2])(f:(A, R1, R2) => Silk[B]) : SilkSeq[B] = macro mFlatMap2With[A, B, R1, R2]

  // For-comprehension
  def foreach[B](f:A=>B) : SilkSeq[B] = macro mForeach[A, B]
  def map[B](f: A => B): SilkSeq[B] = macro mMap[A, B]
  def flatMap[B](f: A => SilkSeq[B]): SilkSeq[B] = macro mFlatMap[A, B]
  def fMap[B](f: A=>GenTraversable[B]) : SilkSeq[B] = macro mFlatMapSeq[A, B]
  def fMapWith[A, B, R1](r1:Silk[R1])(f:(A, R1) => GenTraversable[B]) : SilkSeq[B] = macro mFlatMapSeqWith[A, B, R1]

  // Filtering in for-comprehension
  def filter(cond: A => Boolean): SilkSeq[A] = macro mFilter[A]
  def filterNot(cond: A => Boolean): SilkSeq[A] = macro mFilterNot[A]
  def withFilter(cond: A => Boolean) : SilkSeq[A] = macro mFilter[A] // Use filter

  // Extractor
  def head : SilkSingle[A] = macro mHead[A]
  def collect[B](pf: PartialFunction[A, B]): SilkSeq[B] = macro mCollect[A, B]
  def collectFirst[B](pf: PartialFunction[A, B]): SilkSingle[Option[B]] = macro mCollectFirst[A, B]

  // List operations
  def distinct : SilkSeq[A] = macro mDistinct[A]

  // Block operations
  def split : SilkSeq[SilkSeq[A]] = macro mSplit[A]
  def concat[A <: SilkSeq[B], B] : SilkSeq[B] = macro mConcat[A, B]

  // Grouping
  def groupBy[K](f: A => K): SilkSeq[(K, SilkSeq[A])] = macro mGroupBy[A, K]


  // Aggregators
  def aggregate[B](z: B)(seqop: (B, A) => B, combop: (B, B) => B): SilkSingle[B] = macro mAggregate[A, B]
  def reduce[A1 >: A](f:(A1, A1) => A1) : SilkSingle[A1] = macro mReduce[A1]
  def reduceLeft[B >: A](op: (B, A) => B): SilkSingle[B] = NA // macro mReduceLeft[A, B]
  def fold[A1 >: A](z: A1)(op: (A1, A1) => A1): SilkSingle[A1] = NA // macro mFold[A, A1]
  def foldLeft[B](z: B)(op: (B, A) => B): SilkSingle[B] = NA // macro mFoldLeft[A, B]

  // Scan operations
  /**
   * Scan the elements with an additional variable z (e.g., a counter) , then produce another Silk data set
   * @param z initial value
   * @param op function that produces a pair (new z, another element)
   * @tparam B additional variable (e.g., counter)
   * @tparam C produced element
   */
  def scanLeftWith[B, C](z: B)(op : (B, A) => (B, C)): SilkSeq[C] = NA

  // Shuffle operators are used to describe concrete distributed operations (e.g., GroupBy, HashJoin, etc.)
  def shuffle[A1 >: A](partitioner:Partitioner[A1]) : SilkSeq[(Int, SilkSeq[A1])] = macro mShuffle[A1]
  def shuffleReduce[A <: (Int, SilkSeq[B]), B] : SilkSeq[(Int, SilkSeq[B])] = macro mShuffleReduce[A, B]


  // Joins
  def naturalJoin[B](other: SilkSeq[B])(implicit ev1: ClassTag[A], ev2: ClassTag[B]): SilkSeq[(A, B)] = macro mNaturalJoin[A, B]
  def join[K, B](other: SilkSeq[B], k1: A => K, k2: B => K) : SilkSeq[(A, B)]= macro mJoin[A, K, B]
  //def joinBy[B](other: SilkSeq[B], cond: (A, B) => Boolean) = macro mJoinBy[A, B]


  // Numeric operation
  def sum[A1>:A](implicit num: Numeric[A1]) : SilkSingle[A1] = macro mSum[A1]
  def product[A1 >: A](implicit num: Numeric[A1]) = macro mProduct[A1]
  def min[A1 >: A](implicit cmp: Ordering[A1]) = macro mMin[A1]
  def max[A1 >: A](implicit cmp: Ordering[A1]) = macro mMax[A1]
  def minBy[A1 >: A, B](f: (A1) => B)(implicit cmp: Ordering[B]) = macro mMinBy[A1, B]
  def maxBy[A1 >: A, B](f: (A1) => B)(implicit cmp: Ordering[B]) = macro mMaxBy[A1, B]


  // String
  def mkString(start: String, sep: String, end: String): SilkSingle[String] = macro mMkString[A]
  def mkString(sep: String): SilkSingle[String] = macro mMkStringSep[A]
  def mkString: SilkSingle[String] = macro mMkStringDefault[A]


  // Sampling
  def takeSample(proportion:Double) : SilkSeq[A] = macro mSampling[A]

  // Zipper
  def zip[B](other:SilkSeq[B]) : SilkSeq[(A, B)] = macro mZip[A, B]
  def zipWithIndex : SilkSeq[(A, Int)] = macro mZipWithIndex[A]

  // Sorting
  def sortBy[K](keyExtractor: A => K)(implicit ord: Ordering[K]): SilkSeq[A] = macro mSortBy[A, K]
  def sorted[A1 >: A](partitioner:Partitioner[A])(implicit ord: Ordering[A1]): SilkSeq[A1] = macro mSorted[A1]


  // Operations for gathering distributed data to a node
  /**
   * Collect all distributed data to the node calling this method. This method should be used only for small data.
   */
  def toSeq[A1>:A](implicit env:SilkEnv) : Seq[A1] = get[A1]

  /**
   * Collect all distributed data to the node calling this method. This method should be used only for small data.
   * @tparam A1
   * @return
   */
  def toArray[A1>:A](implicit ev:ClassTag[A1], env:SilkEnv) : Array[A1] = get[A1].toArray

  def toMap[K, V](implicit env:SilkEnv) : Map[K, V] = {
    val entries : Seq[(K, V)] = this.get[A].collect{ case (k, v) => (k -> v).asInstanceOf[(K, V)] }
    entries.toMap[K, V]
  }

  def get[A1>:A](implicit env:SilkEnv) : Seq[A1] = {
    // TODO switch the running cluster according to the env
    env.get(this)
  }

  def get(target:String)(implicit env:SilkEnv) : Any = {
    env.get(this, target)
  }

  def eval(implicit env:SilkEnv) : this.type = {
    env.run(this)
    this
  }

}