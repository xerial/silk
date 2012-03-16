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
// DNA.scala
// Since: 2012/03/16 10:58
//
//--------------------------------------

trait GenomeLetter

/**
 * DNA (A, C, G, T, N) letters
 */
object DNA {
  
  object A extends DNA("A", 0x00, 1)
  object C extends DNA("C", 0x01, 1<<1)
  object G extends DNA("G", 0x02, 1<<2)
  object T extends DNA("T", 0x03, 1<<3)
  object N extends DNA("N", 0x04, 0x0F)

  private[glens] val charToACGTCodeTable : Array[Byte] = Array[Byte](4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
    4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
    4, 4, 4, 4, 4, 4, 4, 4, 0, 4, 1, 4, 4, 4, 2, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 3, 3, 4, 4, 4, 4, 4, 4, 4,
    4, 4, 4, 4, 0, 4, 1, 4, 4, 4, 2, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
    4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
    4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
    4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
    4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4 )
  
  private[glens] val codeTable = Array(A, C, G, T, N, N, N, N)
  private[glens] val charTable = Array('A', 'C', 'G', 'T', 'N' )

  val exceptN = Array(A, C, G, T)
  val values = Array(A, C, G, T, N)

  def complement(code:Int) : DNA = codeTable((~code & 0x03) | (code & 0x04))
  def decode(code:Byte) : DNA = codeTable(code & 0x07)
  def encode(ch:Char) : DNA = decode(to3bitCode(ch))
  def to3bitCode(ch:Char) : Byte = charToACGTCodeTable(ch & 0xFF)
  def to2bitCode(ch:Char) : Byte = (to3bitCode(ch) & 0x03).toByte
}

/**
 * @author leo
 */
sealed abstract class DNA(val letter:String, val code:Int, val bitFlag:Int) extends GenomeLetter {
  assert(code >= 0 && code <= 4)

  def complement : DNA = DNA.complement(code)
  def toChar = DNA.charTable(code)
  def isMatchWith(other:DNA) : Boolean = (this.bitFlag & other.bitFlag) != 0

  /**
   * Get 2-bit code of this DNA. N will be replaced with A
   * @return 2-bit code for A, C, G, T
   */
  val to2bitCode : Byte = (code & 0x03).toByte
}

