//--------------------------------------
//
// RangePartitioner.scala
// Since: 2013/07/02 2:16 PM
//
//--------------------------------------

package xerial.silk.weaver

import scala.collection.SortedMap
import xerial.silk.{SilkEnv, SilkSeq}
import xerial.core.log.LoggerFactory
import xerial.silk.core.Partitioner


/**
 * Histogram-based partitioner. We have to place this code separate from silk-core, since RangePartitioner uses
 * some macro-based codes (in.size, in.takeSample, etc.) to compute histograms.
 * @author Taro L. Saito
 */
class RangePartitioner[A](val numPartitions:Int, in:SilkSeq[A], ascending:Boolean=true)(implicit ord:Ordering[A], @transient env:SilkEnv) extends Partitioner[A] {

  // Histogram from value upper bound -> frequency
  lazy val binIndex = {
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

  override def materialize { binIndex }

  /**
   * Get a partition where `a` belongs
   * @param a
   */
  def partition(a: A) = {
    val p = binIndex.from(a).headOption.map(_._2).getOrElse(numPartitions-1)
    if(ascending) p else numPartitions - p - 1
  }
}