//--------------------------------------
//
// RangePartitioner.scala
// Since: 2013/07/02 2:16 PM
//
//--------------------------------------

package xerial.silk.cluster


import scala.collection.SortedMap
import xerial.silk.{SilkSeq, Partitioner}
import xerial.core.log.LoggerFactory


/**
 * Histogram-based partitioner. This code needs to be in a separate project from silk-core, since it uses
 * macro codes to compute histograms.
 * @author Taro L. Saito
 */
class RangePartitioner[A](val numPartitions:Int, in:SilkSeq[A], ascending:Boolean=true)(implicit ord:Ordering[A]) extends Partitioner[A] {

  // Sampling
  val binIndex = {
    val logger = LoggerFactory(classOf[RangePartitioner[_]])
    val n = in.size.get
    logger.debug(f"input size: $n%,d")

    // Sampling strategy described in
    // TeraByteSort on Apache Hadoop. Owen O'Malley (Yahoo!) May 2008
    val p = math.min(100000.0 / n, 0.05)
    val sample = in.takeSample(math.min(100000.0 / n, 0.05)).toSeq.sorted
    logger.debug(f"sample size: ${sample.size}%,d (proportion:${p*100}%.2f)")
    val w = math.max(1, (sample.size.toDouble / numPartitions).toInt)
    val b = SortedMap.newBuilder[A, Int]
    for((keyIt, i) <- sample.drop(w/2).sliding(1, w).zipWithIndex) {
      b += keyIt.head -> i
    }
    val table = b.result

    logger.debug(table)
    table
  }

  /**
   * Get a partition where `a` belongs
   * @param a
   */
  def partition(a: A) = {
    val p = binIndex.from(a).headOption.map(_._2).getOrElse(numPartitions-1)
    if(ascending) p else numPartitions - p - 1
  }
}