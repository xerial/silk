//--------------------------------------
//
// RangePartitioner.scala
// Since: 2013/07/02 2:16 PM
//
//--------------------------------------

package xerial.silk.cluster


import scala.collection.SortedMap
import xerial.silk.{SilkSeq, Partitioner}


/**
 * Histogram-based partitioner. This code needs to be in a separate project from silk-core, since it uses
 * macro codes to compute histograms.
 * @author Taro L. Saito
 */
class RangePartitioner[A](val numPartitions:Int, in:SilkSeq[A], ascending:Boolean=true)(implicit ord:Ordering[A]) extends Partitioner[A] {

  // Sampling
  val binIndex = {
    val n = in.size.get

    // Sampling strategy described in
    // TeraByteSort on Apache Hadoop. Owen O'Malley (Yahoo!) May 2008
    val sample = in.takeSample(math.min(100000.0 / n, 0.1))
    val b = SortedMap.newBuilder[A, Int]
    for((key, i) <- sample.toSeq.sorted.zipWithIndex) { b += key -> i }
    b.result
  }

  /**
   * Get a partition where `a` belongs
   * @param a
   */
  def partition(a: A) = {
    val bi = binIndex.from(a).headOption.map(_._2).getOrElse(binIndex.size)
    val p = bi % numPartitions
    if(ascending) p else numPartitions - p - 1
  }
}