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


case class LoadFile(fc:FContext, file:File) extends SilkSingle[File] {

  override val id = SilkUtil.newUUIDOf(fc, file.getPath)

  def lines : SilkSeq[String] = ReadLine(SilkUtil.newUUIDOf(fc, id, file.getPath), fc, file)
  def rawLines : SilkSeq[UString] = NA
  def as[A](implicit ev:ClassTag[A]) : SilkSeq[A] = NA
}

//case class LoadBlock[A](fc:FContext, blockID:UUID, dataType:Class[A]) extends SilkSeq[A] {
//  override val id = SilkUtil.newUUIDOf(fc, blockID, dataType)
//}
//case class LoadBlockWithFilter[A](fc:FContext, blockID:UUID, dataType:Class[A], filter:A => Boolean) extends SilkSeq[A] {
//  // TODO need to distinguish from LoadBlock.id
//  override val id = SilkUtil.newUUIDOf(fc, blockID, dataType)
//}



case class ReadLine(override val id:UUID, fc:FContext, file:File) extends SilkSeq[String]


//trait HasInput[A] {
//  self:Silk[_] =>
//  val in : SilkSeq[A]
//
//  override def inputs  = Seq(in)
//}
//
//trait HasSingleInput[A] {
//  self:Silk[_] =>
//  val in : SilkSingle[A]
//
//  override def inputs  = Seq(in)
//}

case class MapWithOp[A, B, R1](fc:FContext, in:SilkSeq[A], r1:Silk[R1], f: (A, R1) => B)
 extends SilkSeq[B]


case class Map2WithOp[A, B, R1, R2](fc:FContext, in:SilkSeq[A], r1:Silk[R1], r2:Silk[R2], f: (A, R1, R2) => B)
  extends SilkSeq[B]

case class FlatMapWithOp[A, B, R1](fc:FContext, in:SilkSeq[A], r1:Silk[R1], f:(A, R1)=> Silk[B])
 extends SilkSeq[B]

case class FlatMap2WithOp[A, B, R1, R2](fc:FContext, in:SilkSeq[A], r1:Silk[R1], r2:Silk[R2], f:(A, R1, R2)=> Silk[B])
  extends SilkSeq[B]

case class FilterOp[A](fc: FContext, in: SilkSeq[A], f: A => Boolean)
  extends SilkSeq[A]

case class MapFilterOp[A, B](fc:FContext, in: SilkSeq[A], f:A=>B, filter: B => Boolean) extends SilkSeq[B]

case class FlatMapOp[A, B](fc: FContext, in: SilkSeq[A], f: A => SilkSeq[B])
  extends SilkSeq[B] {
  def fwrap = f.asInstanceOf[Any => SilkSeq[Any]]
}

case class FlatMapSeqOp[A, B](fc: FContext, in: SilkSeq[A], f: A => GenTraversable[B])
  extends SilkSeq[B] {
  def clean = FlatMapSeqOp(fc, in, ClosureSerializer.cleanupF1(f))
  def fwrap = f.asInstanceOf[Any => GenTraversable[Any]]
}

case class FlatMapSeqWithOp[A, B, R1](fc: FContext, in: SilkSeq[A], r1:Silk[R1], f: (A, R1) => GenTraversable[B])
  extends SilkSeq[B] {
  def clean = FlatMapSeqWithOp[A, B, R1](fc, in, r1, ClosureSerializer.cleanupF1_3(f))
  def fwrap = f.asInstanceOf[Any => GenTraversable[Any]]
}


case class MapOp[A, B](fc: FContext, in: SilkSeq[A], f: A => B)
  extends SilkSeq[B] {
  def clean = MapOp(fc, in, ClosureSerializer.cleanupF1(f))
  def fwrap = f.asInstanceOf[Any => Any]
}

case class ForeachOp[A, B: ClassTag](fc: FContext, in: SilkSeq[A], f: A => B)
  extends SilkSeq[B] {
  def fwrap = f.asInstanceOf[Any => Any]
}

case class GroupByOp[A, K](fc: FContext, in: SilkSeq[A], f: A => K)
  extends SilkSeq[(K, SilkSeq[A])] {
  def fwrap = f.asInstanceOf[Any => Any]
}

case class SamplingOp[A](fc:FContext, in:SilkSeq[A], proportion:Double)
 extends SilkSeq[A]


case class RawSeq[A](override val id:UUID, fc: FContext, in:Seq[A])
  extends SilkSeq[A]
case class RawSingle[A](override val id:UUID, fc: FContext, in:A)
  extends SilkSingle[A]

case class ScatterSeq[A](override val id:UUID, fc:FContext, in:Seq[A], numNodes:Int) extends SilkSeq[A]


case class SizeOp[A](fc:FContext, in:SilkSeq[A]) extends SilkSingle[Long]


case class ShuffleOp[A, K](fc: FContext, in: SilkSeq[A], partitioner: Partitioner[A])
  extends SilkSeq[A]

case class ShuffleReduceOp[A](override val id:UUID, fc: FContext, in: ShuffleOp[A, _], ord:Ordering[A])
  extends SilkSeq[A]

case class ShuffleMergeOp[A, B](fc: FContext, left: SilkSeq[A], right: SilkSeq[B], aProbe: A=> Int, bProbe: B=>Int)
  extends SilkSeq[(Int, SilkSeq[A], SilkSeq[B])]


case class NaturalJoinOp[A: ClassTag, B: ClassTag](fc: FContext, left: SilkSeq[A], right: SilkSeq[B])
  extends SilkSeq[(A, B)] {

  def keyParameterPairs = {
    val lt = ObjectSchema.of[A]
    val rt = ObjectSchema.of[B]
    val lp = lt.constructor.params
    val rp = rt.constructor.params
    for (pl <- lp; pr <- rp if (pl.name == pr.name) && pl.valueType == pr.valueType) yield (pl, pr)
  }
}
case class JoinOp[A, B, K](fc:FContext, left:SilkSeq[A], right:SilkSeq[B], k1:A=>K, k2:B=>K) extends SilkSeq[(A, B)]
//case class JoinByOp[A, B](id:UUID, fc:FContext, left:SilkSeq[A], right:SilkSeq[B], cond:(A, B)=>Boolean) extends SilkSeq[(A, B)]

case class ZipOp[A, B](fc:FContext, left:SilkSeq[A], right:SilkSeq[B])
  extends SilkSeq[(A, B)]

case class MkStringOp[A](fc:FContext, in:SilkSeq[A], start:String, sep:String, end:String)
  extends SilkSingle[String]

case class ZipWithIndexOp[A](fc:FContext, in:SilkSeq[A])
  extends SilkSeq[(A, Int)]

case class NumericFold[A](fc:FContext, in:SilkSeq[A], z: A, op: (A, A) => A) extends SilkSingle[A]
case class NumericReduce[A](fc:FContext, in:SilkSeq[A], op: (A, A) => A) extends SilkSingle[A]

case class SortByOp[A, K](fc:FContext, in:SilkSeq[A], keyExtractor:A=>K, ordering:Ordering[K])
  extends SilkSeq[A]

case class SortOp[A](fc:FContext, in:SilkSeq[A], ordering:Ordering[A], partitioner:Partitioner[A])
  extends SilkSeq[A]


case class SplitOp[A](fc:FContext, in:SilkSeq[A])
  extends SilkSeq[SilkSeq[A]]


case class ConcatOp[A, B](fc:FContext, in:SilkSeq[A], asSeq:A=>Seq[B])
  extends SilkSeq[B]


case class MapSingleOp[A, B : ClassTag](fc: FContext, in:SilkSingle[A], f: A=>B)
  extends SilkSingle[B]


case class FilterSingleOp[A: ClassTag](fc: FContext, in:SilkSingle[A], f: A=>Boolean)
  extends SilkSingle[A]



case class SilkEmpty(fc:FContext) extends SilkSingle[Nothing] {
  override def size = 0

}
case class ReduceOp[A: ClassTag](fc: FContext, in: SilkSeq[A], f: (A, A) => A)
  extends SilkSingle[A]



// data manipulation tasks
case class HeadOp[A](fc:FContext, in:SilkSeq[A]) extends SilkSingle[A]
case class TailOp[A](fc:FContext, in:SilkSeq[A]) extends SilkSingle[A]

// object store tasks
case class SerializeOp[A](fc:FContext, in:SilkSeq[A]) extends SilkSingle[A]


case class SaveObjectOp[A](fc:FContext, in:SilkSeq[A]) extends SilkSingle[A]
