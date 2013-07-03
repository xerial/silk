//--------------------------------------
//
// CallGraph.scala
// Since: 2013/06/16 16:23
//
//--------------------------------------

package xerial.silk.framework.ops

import java.util.UUID
import xerial.silk.Silk

object CallGraph {
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


  def apply[A](op:Silk[A]) = createCallGraph(op)

  def createCallGraph[A](op: Silk[A]) = {
    val g = new CallGraphBuilder

    def loop(node: Silk[_]) {
      if (!g.containNode(node)) {
        for (in <- node.inputs if in != null) {
          loop(in)
          g.addEdge(in, node)
        }
      }
    }
    loop(op)
    g.result
  }

}

/**
 * @author Taro L. Saito
 */
case class CallGraph(nodes: Seq[Silk[_]], edges: Map[UUID, Seq[UUID]]) {

  private val idToSilkTable = nodes.map(x => x.id -> x).toMap
  private val silkToIdTable = nodes.map(x => x -> x.id).toMap

  private def idPrefix(uuid:UUID) = uuid.toString.substring(0, 8)

  override def toString = {
    val s = new StringBuilder
    s append "[nodes]\n"
    for (n <- nodes)
      s append s" $n\n"

    s append "[edges]\n"
    for ((src, lst) <- edges.toSeq.sortBy(_._1); dest <- lst.sorted) {
      s append s" ${idPrefix(src)} -> ${idPrefix(dest)}\n"
    }
    s.toString
  }

  def childrenOf(op:Silk[_]) : Seq[Silk[_]] = {
    val childIDs = edges.getOrElse(op.id, Seq.empty)
    childIDs.map(cid => idToSilkTable(cid))
  }

  def descendantsOf(op:Silk[_]) : Set[Silk[_]] = {
    var seen = Set.empty[Silk[_]]
    def loop(current:Silk[_]) {
      if(seen.contains(current))
        seen += current
      for(child <- childrenOf(current))
        loop(child)
    }
    loop(op)
    seen
  }

}