//--------------------------------------
//
// Sort.scala
// Since: 2012/12/07 2:31 PM
//
//--------------------------------------

package xerial.silk.example

import util.Random
import xerial.silk._
import xerial.silk.cluster.RangePartitioner


/**
 * Sorting example
 * @author Taro L. Saito
 */
class Sort(N:Int = 100000000, numSplits:Int=4, numReducer:Int=3) {

  def run = {
    // Create a random Int sequence
    val input = Silk.scatter(for(i <- 0 until N) yield {Random.nextInt}, numSplits)
    val sorted = input.sorted(new RangePartitioner(numReducer, input))
    sorted
  }

}