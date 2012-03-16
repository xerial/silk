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
// Strand.scala
// Since: 2012/03/16 10:20
//
//--------------------------------------

object Strand {
  val strands = Seq(Forward, Reverse)
}

/**
 * Strand of
 *
 * @author leo
 */
abstract class Strand(val symbol:String) {
  def toInt : Int
  def isForward : Boolean
  def isReverse : Boolean = !isForward
  override def toString = symbol
}

object Forward extends Strand("+") {
  def toInt = 1
  def isForward = true
}

object Reverse extends Strand("-") {
  def toInt = -1
  def isForward = false
}

