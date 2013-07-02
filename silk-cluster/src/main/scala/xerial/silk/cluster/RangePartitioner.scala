//--------------------------------------
//
// RangePartitioner.scala
// Since: 2013/07/02 2:16 PM
//
//--------------------------------------

package xerial.silk.cluster

import xerial.silk.framework.ops.{Partitioner, SilkSeq}
import scala.collection.SortedMap

/**
 * Histogram-based partitioner. This code needs to be in a separate project from silk-core, since it uses
 * macro codes to comput histograms.
 * @author Taro L. Saito
 */
class RangePartitioner[A](val numPartitions:Int, in:SilkSeq[A], ascending:Boolean=true)(implicit ord:Ordering[A]) extends Partitioner[A] {

  // Sampling
  val binIndex = {
    val n = in.size.get
    val sample = in.takeSample(100000.0 / n)
    val b = SortedMap.newBuilder[A, Int]
    for((key, i) <- sample.sorted.zipWithIndex) { b += key -> i }
    b.result
  }

  /**
   * Get a partition where `a` belongs
   * @param a
   */
  def partition(a: A) = {
    val bi = binIndex.from(a).headOption.map(_._2).getOrElse(binIndex.size)
    bi % numPartitions
  }
}