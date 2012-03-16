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
// BED.scala
// Since: 2012/03/16 14:02
//
//--------------------------------------

/**
 * UCSC's BED format
 * @param chr
 * @param chromStart
 * @param chromEnd
 */
class BED(val chr: String, val chromStart: Int, val chromEnd: Int) extends IntervalLike with InChromosome with ZeroOrigin[BED] {
  val start = chromStart
  val end = chromEnd
}

/**
 * @author leo
 */
class BEDGene
(
  chr: String,
  chromStart: Int,
  chromEnd: Int,
  val name: String,
  val score: Int,
  val strand: Strand,
  val thickStart: Int,
  val thickEnd: Int,
  val itemRgb : String,
  val blockCount : Int,
  val blockSizes: Array[Int],
  val blockStarts : Array[Int]
) extends BED(chr, chromStart, chromEnd)
{

  def cdsStart = cdsRange.start
  def cdsEnd = cdsRange.end
  lazy val cdsRange : GInterval = new GInterval(chr, thickStart, thickEnd, strand)
  lazy val exons : Array[GInterval] = {
    for((size, exonStart) <- blockSizes.zip(blockStarts)) yield {
      new GInterval(chr, start, end, strand)
    }
  }
  lazy val cds : Array[GInterval] = {
    for(ex <- exons; c <- ex.intersection(cdsRange)) yield {
      c
    }
  }
}


object BED {

  class ZeroToOne extends Converter[BED, ChrInChromosome] {
    
  } 

}
