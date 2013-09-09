//--------------------------------------
//
// Sort.scala
// Since: 2012/12/07 2:31 PM
//
//--------------------------------------

package xerial.silk.example

import util.Random
import xerial.silk._
import xerial.silk.cluster._
import xerial.silk.cluster.RangePartitioner
import xerial.core.log.Logger


/**
 * Sorting example
 * @author Taro L. Saito
 */
class Sort(zkConnectString:String, N:Int = 100000000, numSplits:Int=4, numReducer:Int=3) extends Logger {

  def run = {
    silkEnv(zkConnectString) {
      // Create a random Int sequence
      val input = Silk.scatter(for(i <- 0 until N) yield {Random.nextInt(N)}, numSplits)
      val sorted = input.sorted(new RangePartitioner(numReducer, input))
      val result = sorted.get
      info(s"sorted: ${result.size} [${result.take(10).mkString(", ")}, ...]")
      result
    }
  }

}