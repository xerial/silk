//--------------------------------------
//
// CallGraph.scala
// Since: 2013/06/16 16:23
//
//--------------------------------------

package xerial.silk.framework.ops

import java.util.UUID

object CallGraph {
  private class CallGraphBuilder {
    var nodeTable = collection.mutable.Map[UUID, SilkMini[_]]()
    var edgeTable = collection.mutable.Map[UUID, Set[UUID]]()

    def containNode[A](n: SilkMini[A]): Boolean = {
      nodeTable.contains(n.uuid)
    }

    def addNode[A](n: SilkMini[A]) {
      nodeTable += n.uuid -> n
    }

    def addEdge[A, B](from: SilkMini[A], to: SilkMini[B]) {
      addNode(from)
      addNode(to)
      val outNodes = edgeTable.getOrElseUpdate(from.uuid, Set.empty)
      edgeTable += from.uuid -> (outNodes + to.uuid)
    }

    def result: CallGraph = {
      new CallGraph(nodeTable.values.toSeq.sortBy(_.uuid), edgeTable.map {
        case (i, lst) => i -> lst.toSeq
      }.toMap)
    }
  }


  def createCallGraph[A](op: SilkMini[A]) = {
    val g = new CallGraphBuilder

    def loop(node: SilkMini[_]) {
      if (!g.containNode(node)) {
        for (in <- node.inputs) {
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
case class CallGraph(nodes: Seq[SilkMini[_]], edges: Map[UUID, Seq[UUID]]) {

  private val idToSilkTable = nodes.map(x => x.uuid -> x).toMap
  private val silkToIdTable = nodes.map(x => x -> x.uuid).toMap

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

  def childrenOf(op:SilkMini[_]) : Seq[SilkMini[_]] = {
    val childIDs = edges.getOrElse(op.uuid, Seq.empty)
    childIDs.map(cid => idToSilkTable(cid))
  }

  def descendantsOf(op:SilkMini[_]) : Set[SilkMini[_]] = {
    var seen = Set.empty[SilkMini[_]]
    def loop(current:SilkMini[_]) {
      if(seen.contains(current))
        seen += current
      for(child <- childrenOf(current))
        loop(child)
    }
    loop(op)
    seen
  }

}