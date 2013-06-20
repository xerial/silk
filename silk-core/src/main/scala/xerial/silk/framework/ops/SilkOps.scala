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

/**
 * This file defines Silk operations
 */


case class LoadFile(override val fc:FContext, file:File) extends SilkSingle[File](fc) {
  def lines : SilkSeq[String] = NA
  def rawLines : SilkSeq[UString] = NA
  def as[A](implicit ev:ClassTag[A]) : SilkSeq[A] = NA
}

case class FilterOp[A: ClassTag](override val fc: FContext, in: SilkSeq[A], f: A => Boolean, @transient fe: ru.Expr[A => Boolean])
  extends SilkSeq[A](fc)

case class FlatMapOp[A, B](override val fc: FContext, in: SilkSeq[A], f: A => SilkSeq[B], @transient fe: ru.Expr[A => SilkSeq[B]])
  extends SilkSeq[B](fc) {

  def fwrap = f.asInstanceOf[Any => SilkSeq[Any]]
}

case class MapOp[A, B](override val fc: FContext, in: SilkSeq[A], f: A => B, @transient fe: ru.Expr[A => B])
  extends SilkSeq[B](fc) {

  def fwrap = f.asInstanceOf[Any => Any]
}

case class SimpleMapOp[A, B](override val fc: FContext, in: SilkSeq[A], f: A => B)
  extends SilkSeq[B](fc) {

  def fwrap = f.asInstanceOf[Any => Any]
}

case class ForeachOp[A, B: ClassTag](override val fc: FContext, in: SilkSeq[A], f: A => B, @transient fe: ru.Expr[A => B])
  extends SilkSeq[B](fc) {
  def fwrap = f.asInstanceOf[Any => Any]
}

case class GroupByOp[A, K](override val fc: FContext, in: SilkSeq[A], f: A => K, @transient fe: ru.Expr[A => K])
  extends SilkSeq[(K, SilkSeq[A])](fc) {

  def fwrap = f.asInstanceOf[Any => Any]

}

case class SamplingOp[A:ClassTag](override val fc:FContext, in:SilkSeq[A], proportion:Double)
 extends SilkSeq[A](fc)


case class RawSeq[+A: ClassTag](override val fc: FContext, @transient in:Seq[A])
  extends SilkSeq[A](fc)

case class ShuffleOp[A: ClassTag, K](override val fc: FContext, in: SilkSeq[A], keyParam: Parameter, partitioner: K => Int)
  extends SilkSeq[A](fc)


case class MergeShuffleOp[A: ClassTag, B: ClassTag](override val fc: FContext, left: SilkSeq[A], right: SilkSeq[B])
  extends SilkSeq[(A, B)](fc) {
  override def inputs = Seq(left, right)
}

case class NaturalJoinOp[A: ClassTag, B: ClassTag](override val fc: FContext, left: SilkSeq[A], right: SilkSeq[B])
  extends SilkSeq[(A, B)](fc) {
  override def inputs = Seq(left, right)

  def keyParameterPairs = {
    val lt = ObjectSchema.of[A]
    val rt = ObjectSchema.of[B]
    val lp = lt.constructor.params
    val rp = rt.constructor.params
    for (pl <- lp; pr <- rp if (pl.name == pr.name) && pl.valueType == pr.valueType) yield (pl, pr)
  }
}
case class JoinOp[A, B, K](override val fc:FContext, left:SilkSeq[A], right:SilkSeq[B], k1:A=>K, k2:B=>K) extends SilkSeq[(A, B)](fc)
//case class JoinByOp[A, B](override val fc:FContext, left:SilkSeq[A], right:SilkSeq[B], cond:(A, B)=>Boolean) extends SilkSeq[(A, B)](fc)

case class ZipOp[A, B](override val fc:FContext, left:SilkSeq[A], right:SilkSeq[B])
  extends SilkSeq[(A, B)](fc)

case class MkStringOp[A](override val fc:FContext, in:SilkSeq[A], start:String, sep:String, end:String)
  extends SilkSingle[String](fc)

case class ZipWithIndexOp[A](override val fc:FContext, in:SilkSeq[A])
  extends SilkSeq[(A, Int)](fc)

case class NumericFold[A](override val fc:FContext, in:SilkSeq[A], z: A, op: (A, A) => A) extends SilkSingle[A](fc)
case class NumericReduce[A](override val fc:FContext, in:SilkSeq[A], op: (A, A) => A) extends SilkSingle[A](fc)

case class SortByOp[A, K](override val fc:FContext, in:SilkSeq[A], keyExtractor:A=>K, ordering:Ordering[K])
  extends SilkSeq[A](fc)

case class SortOp[A](override val fc:FContext, in:SilkSeq[A], ordering:Ordering[A])
  extends SilkSeq[A](fc)


case class NoOp[A](override val fc:FContext)
  extends SilkSeq[A](fc)

case class SplitOp[A](override val fc:FContext, in:SilkSeq[A])
  extends SilkSeq[SilkSeq[A]](fc)

case class ConcatOp[A, B](override val fc:FContext, in:SilkSeq[A], asSilkSeq:A=>SilkSeq[B])
  extends SilkSeq[B](fc)

case class MapSingleOp[A, B : ClassTag](override val fc: FContext, in:SilkSingle[A], f: A=>B, @transient fe: ru.Expr[A=>B])
  extends SilkSingle[B](fc)

case class FilterSingleOp[A: ClassTag](override val fc: FContext, in:SilkSingle[A], f: A=>Boolean, @transient fe: ru.Expr[A=>Boolean])
  extends SilkSingle[A](fc)


case class SilkEmpty(override val fc:FContext) extends SilkSingle[Nothing](fc) {
  override def size = 0

}
case class ReduceOp[A: ClassTag](override val fc: FContext, in: SilkSeq[A], f: (A, A) => A, @transient fe: ru.Expr[(A, A) => A])
  extends SilkSingle[A](fc) {
  override def inputs = Seq(in)
}



