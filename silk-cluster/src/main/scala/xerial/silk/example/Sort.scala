//--------------------------------------
//
// Sort.scala
// Since: 2012/12/07 2:31 PM
//
//--------------------------------------

package xerial.silk.example

import util.Random
import xerial.silk._
import scala.collection.immutable.SortedMap
import xerial.silk.cluster.RangePartitioner


/**
 * Sorting example
 * @author Taro L. Saito
 */
object Sort {

  def blockIndex(v:Int, sIndex:SortedMap[Int, Int]) : Int = {
    // Find i where sample[i-1] <= v < sample[i]
    sIndex.from(v).headOption.map(_._2).getOrElse(sIndex.size)
  }

  def N = 100000000

  def run = {
    // Create a random Int sequence
    val input = (for(i <- 0 until N) yield {
      Random.nextInt
    }).toSilk

    val sorted = input.sorted(new RangePartitioner(10, input))
    sorted
  }

}