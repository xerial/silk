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
import xerial.silk.model.Enum

//--------------------------------------
//
// Strand.scala
// Since: 2012/03/16 10:20
//
//--------------------------------------

object Strand extends Enum[Strand] {
  def values = Array(Forward, Reverse)
  def symbols = Array(Forward.symbol, Reverse.symbol)

  def apply(ch:Char) : Strand = {
    if(ch == '+') Forward else Reverse
  }
  def apply(str:String) : Strand = {
    if(str == "+") Forward else Reverse
  }
}

/**
 * Forward or reverse Strand
 *
 * @author leo
 */
sealed abstract class Strand(val symbol:String) {
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

//object BothStrand extends Strand("=") {
//  def toInt = 0
//  def isForward = false
//  override def isReverse = false
//}
