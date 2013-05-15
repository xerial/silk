//--------------------------------------
//
// SimpleExecutor.scala
// Since: 2013/05/15 10:38
//
//--------------------------------------

package xerial.silk.cluster.scheduler

import xerial.silk.core.{CallGraph, Silk, SilkSingle, SilkExecutor}
import xerial.silk.SilkException
import xerial.core.log.Logger


trait Mark
object Pending extends Mark
object Done extends Mark


/**
 * @author Taro L. Saito
 */
class SimpleExecutor extends SilkExecutor with Logger {
  def eval[A](in: Silk[A]) = throw SilkException.pending

  def evalSingle[A](in: SilkSingle[A]) = {

    val g = CallGraph(in)
    debug(g)

    // Initialize marks
    val marks = collection.mutable.Map[Int, Mark]()
    for(id <- g.nodeIDs) marks += id -> Pending

    // Find nodes without inconming edges
    var ready = g.rootNodeIDs

    while(!ready.isEmpty) {
      val nid = ready.head
      val node = g(nid)
      debug(s"Evaluating $node")

      

      // Remove the evaluated node
      ready -= nid
      marks += nid -> Done

      // Find next ready node
      val nextCandidate =
        for(d <- g.destOf(nid)
            if marks(d) == Pending &&
              g.inputOf(d).forall(i => marks(i) == Done))
        yield d
      ready ++= nextCandidate
    }

    throw SilkException.pending
  }
}