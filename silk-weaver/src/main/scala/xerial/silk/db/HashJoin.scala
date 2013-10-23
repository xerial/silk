//--------------------------------------
//
// HashJoin.scala
// Since: 2013/10/23 3:11 PM
//
//--------------------------------------

package xerial.silk.db

import xerial.silk.{SilkSeq, Silk}

/**
 * Hash-join implementation in Silk
 * @author Taro L. Saito
 */
object HashJoin {

  def apply[A, B](a:SilkSeq[A], b:SilkSeq[B], aProbe: A=>Int, bProbe:B=>Int) : SilkSeq[(A, B)] = {

    import Silk._

    // [a1, a2, ..., an] => [(k1, [ax, ay]), (k2, [aa, ...]), ...]
    val P = 100
    val pa = a.groupBy(x => aProbe(x) % P).toMap[Int, SilkSeq[A]]
    val pb = b.groupBy(x => bProbe(x) % P).toMap[Int, SilkSeq[B]]

    val joined = for(i <- (0 until P).toSilk) yield {
      val ai = pa.getOrElse(i, Silk.empty).get
      val bi = pb.getOrElse(i, Silk.empty).get

      if(ai.isEmpty || bi.isEmpty)
        Seq.empty[(A, B)]
      else {
        val pai = ai.groupBy(aProbe)
        val pbi = bi.groupBy(bProbe).toMap

        val pair = Seq.newBuilder[(A, B)]
        for((k, as) <- pai; va <- as; vb <- pbi.getOrElse(k, Seq.empty)) { pair += ((va, vb)) }
        pair.result
      }
    }

    joined.concat
  }


}