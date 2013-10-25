//--------------------------------------
//
// HashJoin.scala
// Since: 2013/10/23 3:11 PM
//
//--------------------------------------

package xerial.silk.db

import xerial.silk.{Partitioner, SilkSeq, Silk}
import scala.collection.mutable
import xerial.silk.cluster.RangePartitioner
import scala.reflect.ClassTag

/**
 * Hash-join implementation in Silk
 * @author Taro L. Saito
 */
object HashJoin {

  def apply[A, B](a:SilkSeq[A], b:SilkSeq[B], aProbe: A=>Int, bProbe:B=>Int) : SilkSeq[(A, B)] = {

    import Silk._

    // [a1, a2, ..., an] => [(k1, [ax, ay]), (k2, [aa, ...]), ...]
    // TODO determine appropriate partition numbers
    val P = 100
    // Partition the inputs, and merge them in the same location
    val partition = shuffleMerge(a, b, { x : A => aProbe(x) % P }, { x : B => bProbe(x) % P })
    val joined = partition.map{ case (k1, ai, bi) =>
      if(ai.isEmpty || bi.isEmpty)
        Seq.empty[(A, B)]
      else {
        // Re-probe them with the original probe functions
        val pai = ai.get.groupBy(aProbe)
        val pbi = bi.get.groupBy(bProbe).toMap[Int, Seq[B]]

        val pair = Seq.newBuilder[(A, B)]
        for((k2, as) <- pai; va <- as; vb <- pbi.getOrElse(k2, Seq.empty)) { pair += ((va, vb)) }
        pair.result
      }
    }

    joined.concat
  }


}

object GroupBy {

  def apply[A:ClassTag, K](a:SilkSeq[A], aProbe: A => K) : SilkSeq[(K, SilkSeq[A])] = {

    import Silk._
    import xerial.core.util.DataUnit._

    // If the size of a is less than the block size (e.g., 128MB)
    val inputByteSize = a.byteSize.get
    if(inputByteSize < 128 * MB) {
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
      val P = 100
      // Partition the inputs, and merge them in the same location
      val partition = a.shuffle({x:A => aProbe(x).hashCode}, P)
      partition.flatMap{ case (k, as) => as.groupBy(aProbe) }
    }
  }

}

object Sort {

  def apply[A, K](a:SilkSeq[A], ordering: Ordering[A], partitioner:RangePartitioner[A]) : SilkSeq[A] = {
    val shuffle = a.shuffle(partitioner, partitioner.numPartitions)
    val shuffleReduce : SilkSeq[(K, SilkSeq[A])] = shuffle.shuffleReduce[(K, SilkSeq[A]), K, A]
    val sorted = for((partition, lst) <- shuffleReduce) yield {
      val block = lst.get
      block.sorted(ordering)
    }
    sorted.concat
  }

}




