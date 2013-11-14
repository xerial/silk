//--------------------------------------
//
// CallGraph.scala
// Since: 2013/06/16 16:23
//
//--------------------------------------

package xerial.silk.framework.core

import java.util.UUID
import xerial.silk.Silk
import xerial.lens.TypeUtil
import xerial.core.log.Logger
import xerial.silk.util.MacroUtil

object CallGraph extends Logger {
  private class CallGraphBuilder {
    var nodeTable = collection.mutable.Map[UUID, Silk[_]]()
    var edgeTable = collection.mutable.Map[UUID, Set[UUID]]()

    def containNode[A](n: Silk[A]): Boolean = {
      nodeTable.contains(n.id)
    }

    def addNode[A](n: Silk[A]) {
      nodeTable += n.id -> n
    }

    def addEdge[A, B](from: Silk[A], to: Silk[B]) {
      addNode(from)
      addNode(to)
      val outNodes = edgeTable.getOrElseUpdate(from.id, Set.empty)
      edgeTable += from.id -> (outNodes + to.id)
    }

    def result: CallGraph = {
      new CallGraph(nodeTable.values.toSeq.sortBy(_.id), edgeTable.map {
        case (i, lst) => i -> lst.toSeq
      }.toMap)
    }
  }


  def apply[A](op: Silk[A]) = createCallGraph(op)

  import scala.reflect.runtime.{universe => ru}
  import ru._

  def createCallGraph[A](op: Silk[A]): CallGraph = {
    val g = new CallGraphBuilder

    def loop(node: Silk[_]) {
      if (!g.containNode(node)) {
        g.addNode(node)
        // Add edge from input Silk data
        for (in <- node.inputs if in != null) {
          loop(in)
          g.addEdge(in, node)
        }
      }
    }
    loop(op)
    g.result
  }

  def isSilkType[A](cl: Class[A]): Boolean = classOf[Silk[_]].isAssignableFrom(cl)
  private[silk] val mirror = ru.runtimeMirror(Thread.currentThread.getContextClassLoader)

  def zero[A](cl: Class[A]) = cl match {
    case f if isSilkType(f) =>
      Silk.empty
    case _ => TypeUtil.zero(cl)
  }


}

/**
 * @author Taro L. Saito
 */
case class CallGraph(nodes: Seq[Silk[_]], edges: Map[UUID, Seq[UUID]]) {

  private val idToSilkTable = nodes.map(x => x.id -> x).toMap
  private val silkToIdTable = nodes.map(x => x -> x.id).toMap

  private def idPrefix(uuid: UUID) = uuid.toString.substring(0, 8)

  def node(id:UUID) : Silk[_] = idToSilkTable(id)

  override def toString = {
    val s = new StringBuilder
    s append "[nodes]\n"
    for (n <- nodes)
      s append s" $n\n"

    s append "[edges]\n"
    val edgeList = for((src, lst) <- edges; dest <- lst) yield src -> dest
    for((src, dest) <- edgeList.toSeq.sortBy(p => (p._2, p._1))) {
      s append s" [${idPrefix(src)}] ${node(src).fc} -> [${idPrefix(dest)}] ${node(dest).fc}\n"
    }
    s.toString
  }

  def childrenOf(op: Silk[_]): Seq[Silk[_]] = {
    val childIDs = edges.getOrElse(op.id, Seq.empty)
    childIDs.map(cid => idToSilkTable(cid))
  }

  def descendantsOf(op: Silk[_]): Set[Silk[_]] = {
    var seen = Set.empty[Silk[_]]
    def loop(current: Silk[_]) {
      if (seen.contains(current))
        seen += current
      for (child <- childrenOf(current))
        loop(child)
    }
    loop(op)
    seen
  }

}