//--------------------------------------
//
// SilkOps.scala
// Since: 2013/06/16 16:13
//
//--------------------------------------

package xerial.silk.framework.ops

import scala.language.existentials
import scala.language.experimental.macros
import scala.reflect.ClassTag
import xerial.lens.{Parameter, ObjectSchema}
import scala.reflect.runtime.{universe=>ru}
import java.io._
import xerial.silk.SilkException._
import xerial.core.io.text.UString
import java.util.UUID
import xerial.silk.core.ClosureSerializer
import xerial.silk._
import scala.collection.GenTraversable

/**
 * This file defines Silk operations
 */


case class LoadFile(id:UUID, fc:FContext, file:File) extends SilkSingle[File] {
  def lines : SilkSeq[String] = ReadLine(SilkUtil.newUUID, fc, file)
  def rawLines : SilkSeq[UString] = NA
  def as[A](implicit ev:ClassTag[A]) : SilkSeq[A] = NA
}

case class ReadLine(id:UUID, fc:FContext, file:File) extends SilkSeq[String]


trait HasInput[A] {
  self:Silk[_] =>
  val in : SilkSeq[A]

  override def inputs  = Seq(in)
}

trait HasSingleInput[A] {
  self:Silk[_] =>
  val in : SilkSingle[A]

  override def inputs  = Seq(in)
}

case class MapWithOp[A, B, R1](id:UUID, fc:FContext, in:SilkSeq[A], r1:Silk[R1], f: (A, R1) => B)
 extends SilkSeq[B] {

  override def inputs = Seq(in, r1)
}

case class Map2WithOp[A, B, R1, R2](id:UUID, fc:FContext, in:SilkSeq[A], r1:Silk[R1], r2:Silk[R2], f: (A, R1, R2) => B)
  extends SilkSeq[B] {

  override def inputs = Seq[Silk[_]](in, r1, r2)

}

case class FlatMapWithOp[A, B, R1](id:UUID, fc:FContext, in:SilkSeq[A], r1:Silk[R1], f:(A, R1)=> Silk[B])
 extends SilkSeq[B] {
  override def inputs = Seq[Silk[_]](in, r1)
}

case class FlatMap2WithOp[A, B, R1, R2](id:UUID, fc:FContext, in:SilkSeq[A], r1:Silk[R1], r2:Silk[R2], f:(A, R1, R2)=> Silk[B])
  extends SilkSeq[B] {
  override def inputs = Seq[Silk[_]](in, r1, r2)
}

case class FilterOp[A: ClassTag](id:UUID, fc: FContext, in: SilkSeq[A], f: A => Boolean)
  extends SilkSeq[A] with HasInput[A]

case class FlatMapOp[A, B](id:UUID, fc: FContext, in: SilkSeq[A], f: A => SilkSeq[B])
  extends SilkSeq[B]
{
  override def inputs = Seq(in)
  def fwrap = f.asInstanceOf[Any => SilkSeq[Any]]
}

case class FlatMapSeqOp[A, B](id:UUID, fc: FContext, in: SilkSeq[A], f: A => GenTraversable[B])
  extends SilkSeq[B]
{
  override def inputs = Seq(in)
  def clean = FlatMapSeqOp(id, fc, in, ClosureSerializer.cleanupF1(f))
  def fwrap = f.asInstanceOf[Any => GenTraversable[Any]]

}

case class MapOp[A, B](id:UUID, fc: FContext, in: SilkSeq[A], f: A => B)
  extends SilkSeq[B] with HasInput[A]
{
  def clean = MapOp(id, fc, in, ClosureSerializer.cleanupF1(f))
  def fwrap = f.asInstanceOf[Any => Any]
}

case class ForeachOp[A, B: ClassTag](id:UUID, fc: FContext, in: SilkSeq[A], f: A => B)
  extends SilkSeq[B] with HasInput[A]
{
  def fwrap = f.asInstanceOf[Any => Any]
}

case class GroupByOp[A, K](id:UUID, fc: FContext, in: SilkSeq[A], f: A => K)
  extends SilkSeq[(K, SilkSeq[A])] with HasInput[A]
{
  def fwrap = f.asInstanceOf[Any => Any]
}

case class SamplingOp[A](id:UUID, fc:FContext, in:SilkSeq[A], proportion:Double)
 extends SilkSeq[A] with HasInput[A]


case class RawSeq[+A: ClassTag](id:UUID, fc: FContext, in:Seq[A])
  extends SilkSeq[A]

/**
 * Used for small input data that can be send through Akka
 */
case class RawSmallSeq[+A: ClassTag](id:UUID, fc: FContext, in:Seq[A])
 extends SilkSeq[A]


case class SizeOp[A](id:UUID, fc:FContext, in:SilkSeq[A]) extends SilkSingle[Long] with HasInput[A] {

}

case class ShuffleOp[A, K](id:UUID, fc: FContext, in: SilkSeq[A], partitioner: Partitioner[A])
  extends SilkSeq[A] with HasInput[A]

case class ShuffleReduceOp[A](id:UUID, fc: FContext, in: ShuffleOp[A, _], ord:Ordering[A])
  extends SilkSeq[A] with HasInput[A]

case class MergeShuffleOp[A: ClassTag, B: ClassTag](id:UUID, fc: FContext, left: SilkSeq[A], right: SilkSeq[B])
  extends SilkSeq[(A, B)] {
  override def inputs = Seq(left, right)
}

case class NaturalJoinOp[A: ClassTag, B: ClassTag](id:UUID, fc: FContext, left: SilkSeq[A], right: SilkSeq[B])
  extends SilkSeq[(A, B)] {
  override def inputs = Seq(left, right)

  def keyParameterPairs = {
    val lt = ObjectSchema.of[A]
    val rt = ObjectSchema.of[B]
    val lp = lt.constructor.params
    val rp = rt.constructor.params
    for (pl <- lp; pr <- rp if (pl.name == pr.name) && pl.valueType == pr.valueType) yield (pl, pr)
  }
}
case class JoinOp[A, B, K](id:UUID, fc:FContext, left:SilkSeq[A], right:SilkSeq[B], k1:A=>K, k2:B=>K) extends SilkSeq[(A, B)] {
  override def inputs = Seq(left, right)
}
//case class JoinByOp[A, B](id:UUID, fc:FContext, left:SilkSeq[A], right:SilkSeq[B], cond:(A, B)=>Boolean) extends SilkSeq[(A, B)]

case class ZipOp[A, B](id:UUID, fc:FContext, left:SilkSeq[A], right:SilkSeq[B])
  extends SilkSeq[(A, B)] {
  override def inputs = Seq(left, right)
}

case class MkStringOp[A](id:UUID, fc:FContext, in:SilkSeq[A], start:String, sep:String, end:String)
  extends SilkSingle[String] with HasInput[A]

case class ZipWithIndexOp[A](id:UUID, fc:FContext, in:SilkSeq[A])
  extends SilkSeq[(A, Int)] with HasInput[A]

case class NumericFold[A](id:UUID, fc:FContext, in:SilkSeq[A], z: A, op: (A, A) => A) extends SilkSingle[A] with HasInput[A]
case class NumericReduce[A](id:UUID, fc:FContext, in:SilkSeq[A], op: (A, A) => A) extends SilkSingle[A] with HasInput[A]

case class SortByOp[A, K](id:UUID, fc:FContext, in:SilkSeq[A], keyExtractor:A=>K, ordering:Ordering[K])
  extends SilkSeq[A] with HasInput[A]

case class SortOp[A](id:UUID, fc:FContext, in:SilkSeq[A], ordering:Ordering[A], partitioner:Partitioner[A])
  extends SilkSeq[A] with HasInput[A]


case class SplitOp[A](id:UUID, fc:FContext, in:SilkSeq[A])
  extends SilkSeq[SilkSeq[A]]  with HasInput[A]


case class ConcatOp[A, B](id:UUID, fc:FContext, in:SilkSeq[A], asSeq:A=>Seq[B])
  extends SilkSeq[B]  with HasInput[A]


case class MapSingleOp[A, B : ClassTag](id:UUID, fc: FContext, in:SilkSingle[A], f: A=>B)
  extends SilkSingle[B]  with HasSingleInput[A]


case class FilterSingleOp[A: ClassTag](id:UUID, fc: FContext, in:SilkSingle[A], f: A=>Boolean)
  extends SilkSingle[A]  with HasSingleInput[A]



case class SilkEmpty(id:UUID, fc:FContext) extends SilkSingle[Nothing] {
  override def size = 0

}
case class ReduceOp[A: ClassTag](id:UUID, fc: FContext, in: SilkSeq[A], f: (A, A) => A)
  extends SilkSingle[A]  with HasInput[A] {
  override def inputs = Seq(in)
}



