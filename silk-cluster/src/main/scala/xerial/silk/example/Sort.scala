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
  def N = 100000000




  def run = {
    // Create a random Int sequence
    val input = Silk.scatter(for(i <- 0 until N) yield {Random.nextInt}, 20)
    val sorted = input.sorted(new RangePartitioner(10, input))
    sorted
  }

}