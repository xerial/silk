//--------------------------------------
//
// HashJoin.scala
// Since: 2013/10/23 3:11 PM
//
//--------------------------------------

package xerial.silk.db

import xerial.silk.{SilkEnv, SilkSeq, Silk}
import scala.collection.mutable
import scala.reflect.ClassTag
import xerial.core.util.DataUnit._
import xerial.silk.weaver.RangePartitioner
import xerial.silk.core.Partitioner
import xerial.core.log.Logger

/**
 * Hash-join implementation in Silk
 * @author Taro L. Saito
 */
object HashJoin extends Logger {

  def apply[A, B](a:SilkSeq[A], b:SilkSeq[B], aProbe: A=>Int, bProbe:B=>Int)(implicit env:SilkEnv) : SilkSeq[(A, B)] = {

    import Silk._


//    val sizeA = a.byteSize.get
//    val sizeB = b.byteSize.get
//
//    if(sizeA <= 128 * MB && sizeB <= 128*MB) {
    val sizeA = a.size.get
    val sizeB = b.size.get
    if(sizeA == 0 || sizeB == 0)
      return Silk.empty[(A, B)]
    val threshold = 100000
    if(sizeA <= threshold && sizeB <= threshold) {
      // Perform in-memory hash join
      // Materialize the inputs, then re-probe them with the original probe functions
      val pai = a.get.groupBy(aProbe)
      val pbi = b.get.groupBy(bProbe).toMap[Int, Seq[B]]

      val pair = Seq.newBuilder[(A, B)]
      for((k2, as) <- pai; va <- as; vb <- pbi.getOrElse(k2, Seq.empty)) { pair += ((va, vb)) }
      pair.result.toSilk
    }
    else {
      // [a1, a2, ..., an] => [(k1, [ax, ay]), (k2, [aa, ...]), ...]
      // TODO determine appropriate number of partitions
      val P = 10
      // Partition the inputs, and merge them in the same location
      val partition = shuffleMerge(a, b, { x : A => aProbe(x) % P }, { x : B => bProbe(x) % P })
      val joined = partition.flatMap{ case (k1, ai, bi) =>
        if(ai.isEmpty || bi.isEmpty)
          Silk.empty[(A, B)]
        else {
          // Re-probe partition with the original probe functions.
          // TODO ensure termination of this recursion
          HashJoin.apply(ai, bi, aProbe, bProbe)
        }
      }
      joined
    }
  }
}

object GroupBy {

  def apply[A, K](a:SilkSeq[A], aProbe: A => K)(implicit env:SilkEnv) : SilkSeq[(K, SilkSeq[A])] = {

    import Silk._

    // If the size of a is less than the block size (e.g., 128MB)
    val inputByteSize = a.byteSize.get
    if(inputByteSize <= 128 * MB) {
      // Perform in-memory group_by operation
      val as = a.get
      val m = mutable.Map[K, mutable.Builder[A, Seq[A]]]()
      for(x <- as) {
        val k = aProbe(x)
        val b = m.getOrElseUpdate(k, Seq.newBuilder[A])
        b += x
      }
      val r = Seq.newBuilder[(K, SilkSeq[A])]
      for((k, b) <- m.toSeq) { r += ((k, b.result.toSilk)) }
      r.result.toSilk
    }
    else {
      // Distributed shuffle and merge
      // [a1, a2, ..., an] => [(k1, [ax, ay]), (k2, [aa, ...]), ...]
      // TODO determine appropriate partition numbers
      val P = 10
      // Partition the inputs, and merge them in the same location
      val partition = a.shuffle(Partitioner[A](aProbe(_).hashCode, P))
      partition.flatMap{ case (k, as) => as.groupBy(aProbe) }
    }
  }

} 

object Sort {

  def apply[A, K](a:SilkSeq[A], ordering: Ordering[A], partitioner:RangePartitioner[A])(implicit env:SilkEnv) : SilkSeq[A] = {
    val shuffle = a.shuffle(partitioner)
    val shuffleReduce : SilkSeq[(Int, SilkSeq[A])] = shuffle.shuffleReduce
    val sorted = for((partition, lst) <- shuffleReduce) yield {
      val block = lst.get
      block.sorted(ordering)
    }
    sorted.concat
  }

}




