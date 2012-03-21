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

package xerial.silk.util.mining

import collection.{GenTraversableOnce, GenTraversable}

//--------------------------------------
//
// PointVector.scala
// Since: 2012/03/22 0:18
//
//--------------------------------------

trait PointVectorLike[E, +Repr] extends IndexedSeq[E] {
  def +(other: PointVector[E]): Repr
  def -(other: PointVector[E]): Repr
  def *(factor: E): Repr
  def /(factor: E): Repr

  protected def forEachIndex[U, R <: Repr](f: Int => U): R = {
    for (i <- 0 until length) {
      f(i)
    }
    this.asInstanceOf[R]
  }

  override def clone: Repr
}

trait MutablePointVectorLike[E, +Repr] extends PointVector[E] with PointVectorLike[E, Repr] {
  def +=(other: PointVector[E]): Repr
  def -=(other: PointVector[E]): Repr
  def *=(factor: E): Repr
  def /=(factor: E): Repr
  def pow(factor: E): Repr
}

trait PointVector[E] extends PointVectorLike[E, PointVector[E]] {}

trait MutablePointVector[E] extends MutablePointVectorLike[E, MutablePointVector[E]] {}

object DVector {
  def zero(dim: Int): DVector = new DVector(Array.fill(dim)(0.0))
  def fill(dim: Int)(f: => Double): DVector = new DVector(Array.fill(dim)(f))

  implicit def toDVector(array: Array[Double]) = new DVector(array)

}

/**
 * Wrapped array of Double
 *
 * @author leo
 */
class DVector(value: Array[Double]) extends MutablePointVector[Double] with MutablePointVectorLike[Double, DVector] with Traversable[Double] {

  type self = this.type

  def apply(index: Int): Double = value(index)
  def length = value.length

  def +=(other: PointVector[Double]): DVector = forEachIndex(i => value(i) += other(i))
  def -=(other: PointVector[Double]): DVector = forEachIndex(i => value(i) -= other(i))
  def *=(factor: Double): DVector = forEachIndex(i => value(i) *= factor)
  def /=(factor: Double): DVector = forEachIndex(i => value(i) /= factor)
  def pow(factor: Double): DVector = forEachIndex(i => value(i) = Math.pow(value(i), factor))

  def lowerBound(other: PointVector[Double]): DVector = forEachIndex(i => value(i) = Math.min(value(i), other(i)))
  def upperBound(other: PointVector[Double]): DVector = forEachIndex(i => value(i) = Math.max(value(i), other(i)))

  override def clone: DVector = new DVector(value.clone)

  def +(other: PointVector[Double]): DVector = clone += other
  def -(other: PointVector[Double]): DVector = clone += other
  def *(factor: Double): DVector = clone *= factor
  def /(factor: Double): DVector = clone /= factor

  override def hashCode = {
    val h = new util.MurmurHash[Double]("DVector".hashCode)
    value.foreach(h)
    h.hash
  }

  override def equals(that: Any): Boolean = that match {
    case that: DVector => (that canEqual this) && (this sameElements that)
    case _ => false
  }

  override def toString = value.toString


}