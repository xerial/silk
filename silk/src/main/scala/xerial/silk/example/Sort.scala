//--------------------------------------
//
// Sort.scala
// Since: 2012/12/07 2:31 PM
//
//--------------------------------------

package xerial.silk.example

import util.Random
import xerial.silk.collection.Silk

/**
 * Sorting example
 * @author Taro L. Saito
 */
object Sort {

  def main(args:Array[String]) = {

    val N = 100000000
    val input = (for(i <- 0 until N) yield {
      Random.nextInt
    }).toArray


    val s : Silk[Int] = Silk.toSilkArray(input)
    s.sorted
  }

}