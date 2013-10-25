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
        // Materialize the inputs then re-probe them with the original probe functions
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

  def apply[A, K](a:SilkSeq[A], aProbe: A => K) : SilkSeq[(K, SilkSeq[A])] = {
    val shuffle = a.shuffle(aProbe)
    val shuffleReduce : SilkSeq[(K, SilkSeq[A])] = shuffle.shuffleReduce[(K, SilkSeq[A]), K, A]
    shuffleReduce
  }

}

object Sort {

  def apply[A, K](a:SilkSeq[A], ordering: Ordering[A], partitioner:RangePartitioner[A]) : SilkSeq[A] = {
    val shuffle = a.shuffle(x => partitioner.partition(x))
    val shuffleReduce : SilkSeq[(K, SilkSeq[A])] = shuffle.shuffleReduce[(K, SilkSeq[A]), K, A]
    val sorted = for((partition, lst) <- shuffleReduce) yield {
      val block = lst.get
      block.sorted(ordering)
    }
    sorted.concat
  }

}




