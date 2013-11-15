//--------------------------------------
//
// SilkOps.scala
// Since: 2013/06/16 16:13
//
//--------------------------------------

/**
 * This package defines Silk operations
 */
package xerial.silk.core

import scala.language.existentials
import scala.reflect.ClassTag
import xerial.lens.ObjectSchema
import xerial.silk.SilkException._
import xerial.core.io.text.UString
import java.util.UUID
import xerial.silk._
import scala.collection.GenTraversable
import java.io.File


case class LoadFile(id:UUID, fc:FContext, file:File) extends SilkSingle[File] {

  //override val id = SilkUtil.newUUIDOf(fc, file.getPath)

  def lines : SilkSeq[String] = ReadLine(SilkUtil.newUUIDOf(classOf[ReadLine], fc, id, file.getPath), fc, file)
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



case class ReadLine(id:UUID, fc:FContext, file:File) extends SilkSeq[String]

case class MapWithOp[A, B, R1](id:UUID, fc:FContext, in:SilkSeq[A], r1:Silk[R1], f: (A, R1) => B) extends SilkSeq[B]
case class Map2WithOp[A, B, R1, R2](id:UUID, fc:FContext, in:SilkSeq[A], r1:Silk[R1], r2:Silk[R2], f: (A, R1, R2) => B) extends SilkSeq[B]
case class FlatMapWithOp[A, B, R1](id:UUID, fc:FContext, in:SilkSeq[A], r1:Silk[R1], f:(A, R1)=> Silk[B]) extends SilkSeq[B]
case class FlatMap2WithOp[A, B, R1, R2](id:UUID, fc:FContext, in:SilkSeq[A], r1:Silk[R1], r2:Silk[R2], f:(A, R1, R2)=> Silk[B]) extends SilkSeq[B]
case class FilterOp[A](id:UUID, fc: FContext, in: SilkSeq[A], f: A => Boolean) extends SilkSeq[A]
case class MapFilterOp[A, B](id:UUID, fc:FContext, in: SilkSeq[A], f:A=>B, filter: B => Boolean) extends SilkSeq[B]
case class FlatMapFilterOp[A, B](id:UUID, fc:FContext, in: SilkSeq[A], f:A=>GenTraversable[B], filter: B => Boolean) extends SilkSeq[B]

case class FlatMapOp[A, B](id:UUID, fc: FContext, in: SilkSeq[A], f: A => SilkSeq[B]) extends SilkSeq[B]
case class FlatMapSeqOp[A, B](id:UUID, fc: FContext, in: SilkSeq[A], f: A => GenTraversable[B]) extends SilkSeq[B]
case class FlatMapSeqWithOp[A, B, R1](id:UUID, fc: FContext, in: SilkSeq[A], r1:Silk[R1], f: (A, R1) => GenTraversable[B]) extends SilkSeq[B]
case class MapOp[A, B](id:UUID, fc: FContext, in: SilkSeq[A], f: A => B) extends SilkSeq[B]
case class ForeachOp[A, B: ClassTag](id:UUID, fc: FContext, in: SilkSeq[A], f: A => B) extends SilkSeq[B]
case class GroupByOp[A, K](id:UUID, fc: FContext, in: SilkSeq[A], f: A => K) extends SilkSeq[(K, SilkSeq[A])]

case class SamplingOp[A](id:UUID, fc:FContext, in:SilkSeq[A], proportion:Double) extends SilkSeq[A]
case class RawSeq[A](id:UUID, fc: FContext, in:Seq[A]) extends SilkSeq[A]
case class RawSingle[A](id:UUID, fc: FContext, in:A) extends SilkSingle[A]
case class ScatterSeq[A](id:UUID, fc:FContext, in:Seq[A], numNodes:Int) extends SilkSeq[A]
case class SizeOp[A](id:UUID, fc:FContext, in:SilkSeq[A]) extends SilkSingle[Long]

case class ShuffleOp[A](id:UUID, fc: FContext, in: SilkSeq[A], partitioner: Partitioner[A]) extends SilkSeq[(Int, SilkSeq[A])]
case class ShuffleReduceOp[A, B](id:UUID, fc: FContext, in: SilkSeq[A]) extends SilkSeq[(Int, SilkSeq[B])]
case class ShuffleReduceSortOp[A](id:UUID, fc: FContext, in: ShuffleOp[A], ord:Ordering[A]) extends SilkSeq[A]
case class ShuffleMergeOp[A, B](id:UUID, fc: FContext, left: SilkSeq[A], right: SilkSeq[B], aProbe: A=> Int, bProbe: B=>Int) extends SilkSeq[(Int, SilkSeq[A], SilkSeq[B])]

case class NaturalJoinOp[A: ClassTag, B: ClassTag](id:UUID, fc: FContext, left: SilkSeq[A], right: SilkSeq[B])
  extends SilkSeq[(A, B)] {

  def keyParameterPairs = {
    val lt = ObjectSchema.of[A]
    val rt = ObjectSchema.of[B]
    val lp = lt.constructor.params
    val rp = rt.constructor.params
    for (pl <- lp; pr <- rp if (pl.name == pr.name) && pl.valueType == pr.valueType) yield (pl, pr)
  }
}

case class JoinOp[A, B, K](id:UUID, fc:FContext, left:SilkSeq[A], right:SilkSeq[B], k1:A=>K, k2:B=>K) extends SilkSeq[(A, B)]
//case class JoinByOp[A, B](id:UUID, fc:FContext, left:SilkSeq[A], right:SilkSeq[B], cond:(A, B)=>Boolean) extends SilkSeq[(A, B)]

case class ZipOp[A, B](id:UUID, fc:FContext, left:SilkSeq[A], right:SilkSeq[B]) extends SilkSeq[(A, B)]
case class MkStringOp[A](id:UUID, fc:FContext, in:SilkSeq[A], start:String, sep:String, end:String) extends SilkSingle[String]
case class ZipWithIndexOp[A](id:UUID, fc:FContext, in:SilkSeq[A]) extends SilkSeq[(A, Int)]
case class NumericFold[A](id:UUID, fc:FContext, in:SilkSeq[A], z: A, op: (A, A) => A) extends SilkSingle[A]
case class NumericReduce[A](id:UUID, fc:FContext, in:SilkSeq[A], op: (A, A) => A) extends SilkSingle[A]
case class SortByOp[A, K](id:UUID, fc:FContext, in:SilkSeq[A], keyExtractor:A=>K, ordering:Ordering[K]) extends SilkSeq[A]
case class SortOp[A](id:UUID, fc:FContext, in:SilkSeq[A], ordering:Ordering[A], partitioner:Partitioner[A]) extends SilkSeq[A]

case class SplitOp[A](id:UUID, fc:FContext, in:SilkSeq[A]) extends SilkSeq[SilkSeq[A]]
case class ConcatOp[A, B](id:UUID, fc:FContext, in:SilkSeq[A]) extends SilkSeq[B]

case class MapSingleOp[A, B](id:UUID, fc: FContext, in:SilkSingle[A], f: A=>B) extends SilkSingle[B]
case class FilterSingleOp[A](id:UUID, fc: FContext, in:SilkSingle[A], f: A=>Boolean) extends SilkSingle[A]

case class SilkEmpty(id:UUID, fc:FContext) extends SilkSingle[Nothing] {
  override def size = 0
}

case class ReduceOp[A:ClassTag](id:UUID, fc: FContext, in: SilkSeq[A], f: (A, A) => A) extends SilkSingle[A]

// data manipulation tasks
case class HeadOp[A](id:UUID, fc:FContext, in:SilkSeq[A]) extends SilkSingle[A]
case class TailOp[A](id:UUID, fc:FContext, in:SilkSeq[A]) extends SilkSingle[A]
case class CollectOp[A, B](id:UUID, fc:FContext, in:SilkSeq[A], pf:PartialFunction[A, B]) extends SilkSeq[B]
case class CollectFirstOp[A, B](id:UUID, fc:FContext, in:SilkSeq[A], pf:PartialFunction[A, B]) extends SilkSingle[Option[B]]


// object store tasks
case class SerializeOp[A](id:UUID, fc:FContext, in:SilkSeq[A]) extends SilkSingle[A]
case class SaveObjectOp[A](id:UUID, fc:FContext, in:SilkSeq[A]) extends SilkSingle[A]


case class DistinctOp[A](id:UUID, fc:FContext, in:SilkSeq[A]) extends SilkSeq[A]

case class AggregateOp[A, B](id:UUID, fc:FContext, in:SilkSeq[A], z:B, seqop:(B,A)=>B, combop:(B, B)=>B) extends SilkSingle[B]
