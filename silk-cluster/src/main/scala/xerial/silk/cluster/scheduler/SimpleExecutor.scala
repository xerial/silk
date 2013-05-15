//--------------------------------------
//
// SimpleExecutor.scala
// Since: 2013/05/15 10:38
//
//--------------------------------------

package xerial.silk.cluster.scheduler

import xerial.silk.core._
import xerial.silk.SilkException
import xerial.core.log.Logger
import scala.reflect.runtime.{universe=>ru}

trait Mark
object Pending extends Mark
object Done extends Mark


/**
 * @author Taro L. Saito
 */
class SimpleExecutor extends SilkExecutor with Logger {

  import SilkFlow._

  def eval[A](in: Silk[A]) = {
    evalImpl(in).asInstanceOf[Seq[A]]
  }

  def evalSingle[A](in: SilkSingle[A]) = {
    evalImpl(in).asInstanceOf[A]
  }

  private def evalImpl[A](in:Silk[A]) = {
    val g = CallGraph(in)
    debug(g)

    // Initialize marks
    val marks = collection.mutable.Map[Int, Mark]()
    for(id <- g.nodeIDs) marks += id -> Pending

    val values = collection.mutable.Map[Int, Any]()

    // Find nodes that have no incoming edges
    var ready = g.rootNodeIDs

    // Start evaluation
    var last : Option[Int] = None
    while(!ready.isEmpty) {
      val nid = ready.head
      val node = g(nid)
      debug(s"Evaluating [$nid]:$node")

      def getInput = {
        val input = g.inputOf(nid)
        if(input.size != 1)
          warn(s"multiple input is found: $input")
        values.get(input.head)
      }

      def unsupported[A](v:A) = warn(s"unsupported input type: $v")

      node.flow match {
        case RawInput(v) =>
          values += nid -> v
        case MapFun(prev, f, fExpr) =>
          for(v <- getInput) {
            v match {
              case s:Seq[_] =>
                values += nid -> s.map(f)
              case _ => unsupported(v)
            }
          }
        case NumericFold(prev, z, op) =>
          for(v <- getInput) {
            v match {
              case s:Seq[_] =>
                val result = s.fold(z)(op.asInstanceOf[(Any, Any) => Any])
                values += nid -> result
              case _ => unsupported(v)
            }
          }
        case _ => warn(s"evaluation code is N/A for ${node}")
      }

      // Remove the evaluated node
      last = Some(nid)
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

    last match {
      case Some(id) =>
        val v = values(id)
        debug(s"result: $v")
        v
      case _ =>
        throw SilkException.pending
    }
  }
}