/*
 * Copyright 2012 Taro L. Saito
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package xerial.silk.glens

import collection.generic.CanBuildFrom

//--------------------------------------
//
// GenomeRange.scala
// Since: 2012/03/16 10:25
//
//--------------------------------------

object GenomeRange {

  //implicit def toInterval(locus: GenomicLocus): GInterval = new GInterval(locus.chr, locus.start, locus.start, locus.strand)

}

sealed trait Origin {
  def isZeroOrigin: Boolean
  def isOneOrigin: Boolean
}

/**
 * A base trait for converting objects to another type
 * @tparam From the type of the original data
 * @tparam Diff the type of the differnce from the original data
 * @tparam To the type of the output to be created
 */
trait Converter[-From, -Diff, +To] {
  def apply(from: From, diff: Diff): To
}

//trait ZeroOrigin[Repr <: IntervalLike[Repr]] extends Origin {
//  private val repr : Repr = this.asInstanceOf[Repr]
//
//  def toOneOrigin[B <: OneOrigin](implicit c:Converter[Repr, Int, B]) : B = {
//    c(repr, 1)
//  }
//  def isZeroOrigin: Boolean = true
//  def isOneOrigin: Boolean = false
//}
//
//
//trait OneOrigin[Repr <: IntervalLike[Repr]] extends Origin {
//  private val repr : Repr = this.asInstanceOf[Repr]
//
//  def toZeroOrigin[B <: ZeroOrigin](implicit conv:Converter[Repr, Int, B]) : B = {
//    conv(repr, -1)
//  }
//  def isZeroOrigin: Boolean = false
//  def isOneOrigin: Boolean = true
//}

//class ZeroToOneOriginConverter[A <: ZeroOrigin[_], B <: OneOrigin[_]] extends Converter[A, Int, B] {
//  def apply(from: A, diff: Int) = from.move(from.start + diff, from.end + diff)
//}

/**
 * Semi-open interval [start, end)
 */
trait IntervalLike[Repr <: IntervalLike[_]] {
  val start: Int
  val end: Int

  if (start > end)
    throw new IllegalArgumentException("invalid range: %s".format(this.toString))

  def length: Int = end - start

  /**
   * Detect overlap with the interval, including containment
   * @param other
   * @return
   */
  def intersectWith[A <: Repr](other: A): Boolean = {
    start < other.end && other.start <= end
  }
  def intersection[A <: Repr](other: A): Option[Repr] = {
    val s = Math.max(start, other.start)
    val e = Math.min(end, other.end)
    if (s <= e)
      Some(extend(s, e))
    else
      None
  }

  def contains[A <: Repr](other: A): Boolean = {
    start <= other.start && other.end <= end
  }

  def extend(newStart: Int, newEnd: Int): Repr

  override def toString = "[%,d, %,d)".format(start, end)
}

trait InChromosome {
  val chr: String
}

trait HasStrand {
  val strand: Strand
}

case class Interval(start: Int, end: Int) extends IntervalLike[Interval] {
  def extend(newStart: Int, newEnd: Int) = new Interval(newStart, newEnd)
}

case class IntervalWithChr(chr: String, start: Int, end: Int) extends IntervalLike[IntervalWithChr] with InChromosome {
  def extend(newStart: Int, newEnd: Int) = new IntervalWithChr(chr, newStart, newEnd)
}

//
//class LocusToIntervalConverter[Locus <:GenomicLocus, To] extends Converter[Locus, Interval, To] {
//  def apply(from: Locus, diff: Interval) = {
//    from.
//  }
//}

/**
 * Locus in genome sequence with chr and strand information
 */
trait GenomicLocus[Repr, RangeRepr] extends InChromosome with HasStrand {
  val start: Int

  /**
   *
   * @param width
   * @return
   */
  def around(width: Int): RangeRepr = extend(start - width, start + width)
  def around(upstreamLength: Int, downstreamLength: Int) = strand match {
    case Forward => extend(start - upstreamLength, start + downstreamLength)
    case Reverse => extend(start - downstreamLength, start + upstreamLength)
  }
  def upstream(length: Int): RangeRepr = strand match {
    case Forward => extend(start - length, start)
    case Reverse => extend(start, start + length)
  }
  def downstream(length: Int): RangeRepr = strand match {
    case Forward => extend(start, start + length)
    case Reverse => extend(start - length, start)
  }

  def extend(newStart: Int, newEnd: Int): RangeRepr
}

/**
 * Locus in genome sequences with chr and strand information
 */
trait GenomicInterval[Repr <: GenomicInterval[_]] extends IntervalLike[Repr] with InChromosome with HasStrand {

  def inSameChr[A <: InChromosome](other: A): Boolean = this.chr == other.chr

  def checkChr[A <: InChromosome, B](other: A, success: => B, fail: => B): B = {
    if (inSameChr(other))
      success
    else
      fail
  }

  def fivePrimeEnd: GLocus = strand match {
    case Forward => new GLocus(chr, start, strand)
    case Reverse => new GLocus(chr, end, strand)
  }

  def threePrimeEnd: GLocus = strand match {
    case Forward => new GLocus(chr, end, strand)
    case Reverse => new GLocus(chr, start, strand)
  }

  override def intersectWith[A <: Repr](other: A): Boolean = {
    checkChr(other, super.intersectWith(other), false)
  }

  override def contains[A <: Repr](other: A): Boolean = {
    checkChr(other, super.intersectWith(other), false)
  }

  override def intersection[A <: Repr](other: A): Option[Repr] = {
    checkChr(other, super.intersection(other), None)
  }

  def extend(newStart: Int, newEnd: Int): Repr
}

case class GLocus(val chr: String, val start: Int, val strand: Strand) extends GenomicLocus[GLocus, GInterval] {
  override def toString = "%s:%,d:%s".format(chr, start, strand)
  def move(newStart: Int) = new GLocus(chr, newStart, strand)
  def extend(newStart: Int, newEnd: Int) = new GInterval(chr, newStart, newEnd, strand)
}

/**
 * @author leo
 */
case class GInterval(val chr: String, val start: Int, val end: Int, val strand: Strand) extends GenomicInterval[GInterval] {
  override def toString = "%s:[%,d, %,d):%s".format(chr, start, end, strand)
  def extend(newStart: Int, newEnd: Int) = new GInterval(chr, newStart, newEnd, strand)
}



