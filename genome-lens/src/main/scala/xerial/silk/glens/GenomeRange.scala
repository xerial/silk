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

import xerial.silk.util.HashKey

//--------------------------------------
//
// GenomeRange.scala
// Since: 2012/03/16 10:25
//
//--------------------------------------

object GenomeRange {


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

/**
 * A common trait for interval classes having [start, end) parameters
 */
trait GenericInterval extends HashKey {
  val start: Int
  val end: Int

  if (start > end)
    throw new IllegalArgumentException("invalid range: %s".format(this.toString))

  override def toString = "[%d, %d)".format(start, end)

  def length: Int = end - start

  def apply(idx: Int): Int = start + idx

  /**
   * Detect overlap with the interval, including containment
   * @param other
   * @return
   */
  def intersectWith(other: GenericInterval): Boolean = {
    start < other.end && other.start <= end
  }

  def contains(other: GenericInterval): Boolean = {
    start <= other.start && other.end <= end
  }

  def contains(pos: Int): Boolean = {
    start <= pos && pos < end
  }

}

/**
 * Operations that are common for semi-open intervals [start, end)
 */
trait IntervalOps[Repr <: IntervalOps[_]] extends GenericInterval {

  /**
   * Take the intersection of two intervals
   * @param other
   * @return
   */
  def intersection(other: GenericInterval): Option[Repr] = {
    val s = math.max(start, other.start)
    val e = math.min(end, other.end)
    if (s <= e)
      Some(newRange(s, e))
    else
      None
  }

  /**
   * Create a new interval based on this interval instance
   * @param newStart
   * @param newEnd
   * @return
   */
  def newRange(newStart: Int, newEnd: Int): Repr


}

trait InChromosome {
  val chr: String
}

trait HasStrand {
  val strand: Strand
}


/**
 * An interval
 * @param start
 * @param end
 */
class Interval(val start: Int, val end: Int) extends IntervalOps[Interval] {
  def newRange(newStart: Int, newEnd: Int) = new Interval(newStart, newEnd)
}

/**
 * An interval with chromosome name. This type of interval is frequently used in genome sciences
 * @param chr
 * @param start
 * @param end
 */
class IntervalWithChr(val chr: String, val start: Int, val end: Int)
  extends IntervalOps[IntervalWithChr] with InChromosome {
  def newRange(newStart: Int, newEnd: Int) = new IntervalWithChr(chr, newStart, newEnd)
}



/**
 * Locus in a genome sequence with chr and strand information
 */
trait GenomicLocus[Repr, RangeRepr] extends InChromosome with HasStrand with HashKey {
  val start: Int

  /**
   *
   * @param width
   * @return
   */
  def around(width: Int): RangeRepr = newRange(start - width, start + width)
  def around(upstreamLength: Int, downstreamLength: Int) = strand match {
    case Forward => newRange(start - upstreamLength, start + downstreamLength)
    case Reverse => newRange(start - downstreamLength, start + upstreamLength)
  }
  def upstream(length: Int): RangeRepr = strand match {
    case Forward => newRange(start - length, start)
    case Reverse => newRange(start, start + length)
  }
  def downstream(length: Int): RangeRepr = strand match {
    case Forward => newRange(start, start + length)
    case Reverse => newRange(start - length, start)
  }

  def toRange: RangeRepr

  def newRange(newStart: Int, newEnd: Int): RangeRepr

  def -[A <: GenomicLocus[_, _]](other:A) :Int = {
    this.start - other.start
  }

  def +[A <: GenomicLocus[_, _]](other:A) :Int = {
    this.start + other.start
  }


  def distanceTo[A <: GenomicLocus[_, _]](other:A) :Int = {
    other.start - this.start
  }
}

/**
 * Common trait for locus classes in genome sequences with chr and strand information
 */
trait GenomicInterval[Repr <: GenomicInterval[_]]
  extends IntervalOps[Repr] with InChromosome with HasStrand {

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

  def intersectWith[A <: Repr](other: A): Boolean = {
    checkChr(other, super.intersectWith(other), false)
  }

  def contains[A <: Repr](other: A): Boolean = {
    checkChr(other, super.intersectWith(other), false)
  }

  def intersection[A <: Repr](other: A): Option[Repr] = {
    checkChr(other, super.intersection(other), None)
  }

  def newRange(newStart: Int, newEnd: Int): Repr

}

/**
 * Locus in genome
 * @param chr
 * @param start
 * @param strand
 */
case class GLocus(val chr: String, val start: Int, val strand: Strand)
  extends GenomicLocus[GLocus, GInterval] {
  override def toString = "%s:%d:%s".format(chr, start, strand)
  def move(newStart: Int) = new GLocus(chr, newStart, strand)
  def newRange(newStart: Int, newEnd: Int) = new GInterval(chr, newStart, newEnd, strand)
  def toRange = new GInterval(chr, start, start, strand)
}

/**
 * Range in genome sequences
 * @author leo
 */
case class GInterval(val chr: String, val start: Int, val end: Int, val strand: Strand)
  extends GenomicInterval[GInterval] {
  override def toString = "%s:[%d, %d):%s".format(chr, start, end, strand)
  def newRange(newStart: Int, newEnd: Int) = new GInterval(chr, newStart, newEnd, strand)
}



