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

//--------------------------------------
//
// GenomeRange.scala
// Since: 2012/03/16 10:25
//
//--------------------------------------

object GenomeRange {

  implicit def toInterval(locus: Locus): GInterval = new GInterval(locus.start, locus.start, locus.strand)

}

/**
 * [start, end)
 */
trait Interval {
  val start: Int
  val end: Int

  if (start > end)
    throw new IllegalArgumentException("invalid range: %s".format(this.toString))

  /**
   * Detect overlap with the interval, including containment
   * @param other
   * @return
   */
  def overlapWith(other: Interval): Boolean = {
    start < other.end && other.start <= end
  }

  def contains(other:Interval) : Boolean = {
    start <= other.start && other.end <= end
  }

  /**
   * Detect intersection with the other interval, except containment state
   * @param other
   * @return
   */
  def intersectWith(other: Interval): Boolean = {
    if (start < other.start)
      other.start < end
    else
      start < other.end
  }

  override def toString = "[%,d, %,d)".format(start, end)

}

trait Locus {
  val start: Int
  val strand: Strand

  def upstream(length: Int): GInterval = strand match {
    case Forward => new GInterval(start - length, start, strand)
    case Reverse => new GInterval(start, start + length, strand)
  }
  def downStream(length: Int): GInterval = strand match {
    case Forward => new GInterval(start, start + length, strand)
    case Reverse => new GInterval(start - length, start, strand)
  }

}

class GLocus(val start: Int, val strand: Strand) extends Locus {
  override def toString = "%,d:%s".format(start, strand)
}

/**
 * @author leo
 */
class GInterval(val start: Int, val end: Int, val strand: Strand) extends Locus with Interval {
  override def toString = "[%,d, %,d):%s".format(start, end, strand)
}