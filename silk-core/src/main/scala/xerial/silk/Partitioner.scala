//--------------------------------------
//
// Partitioner.scala
// Since: 2013/07/02 1:36 PM
//
//--------------------------------------

package xerial.silk


object Partitioner {
}


/**
 * Partitioner
 *
 * @author Taro L. Saito
 */
trait Partitioner[A] extends Serializable {
  /**
   * The maximum number of partitions
   */
  def numPartitions: Int

  /**
   * Get a partition where a belongs
   * @param a
   */
  def partition(a:A) : Int
}




/**
 * Hashing-based partitioner
 * @param numPartitions
 * @tparam A
 */
class HashPartitioner[A](val numPartitions:Int) extends Partitioner[A] {
  require(numPartitions > 0)

  /**
   * Get a partition where `a` belongs
   * @param a
   */
  def partition(a: A) = {
    val hash = a match {
      case null => 0
      case _ => a.hashCode % numPartitions
    }

    if(hash < 0)
      hash + numPartitions
    else
      hash
  }
}




