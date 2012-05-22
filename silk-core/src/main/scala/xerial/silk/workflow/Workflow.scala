//--------------------------------------
//
// Workflow.scala
// Since: 2012/05/22 10:11 AM
//
//--------------------------------------

package xerial.silk.workflow

import java.io.StringWriter
import java.io.PrintWriter
import java.io.PrintStream
import java.io.OutputStream
import java.io.ByteArrayOutputStream
import xerial.silk.util.HashKey


object Graph {

  class Node(val label: String) extends HashKey {
    override def clone: Node = new Node(label)
  }

  class Edge(val src: Node, val dest: Node) extends HashKey {
    override def clone: Edge = new Edge(src.clone, dest.clone)
  }

  def singleton(label: String) = {
    val node = new Node(label)
    new Graph(Array(node), Array.empty, Array(node), Array(node))
  }

  def apply(label:String) = singleton(label)

}

import Graph._


/**
 * @author leo
 */
class Graph(val node: Array[Node],
            val edge: Array[Edge],
            val in: Array[Node],
            val out: Array[Node]) {

  //lazy val nodeIndex = node.map(v => v.label -> v).toMap
  //def getNode(label:Int) = nodeIndex(label)

  override def toString = toGraphviz

  def inEdges(v:Node) = edge.filter(_.dest eq v)
  def outEdges(v:Node) = edge.filter(_.src eq v)

  def isInNode(v: Node) = in.contains(v)

  def isOutNode(v: Node) = out.contains(v)

  def toGraphviz: String = {
    val s = new ByteArrayOutputStream()
    val out = new GraphvizWriter(s)

    out.digraph("G") {
      g =>
        for (n <- node) {
          val option =
            if (isInNode(n))
              Map("shape" -> "box")
            else if (isOutNode(n))
              Map("shape" -> "box", "fillcolor" -> "grey")
            else
              Map.empty[String, String]

          g.node(n.label, option)
        }

        for ((e, i) <- edge.zipWithIndex)
          g.arrow(e.src.label, e.dest.label)
    }
    out.close
    s.toString
  }

  /**
   * Create n copies of this graph
   * @param n
   * @return
   */
  def replicate(n: Int): Graph = {
    val newNodeSet = Array.newBuilder[Node]
    val newEdgeSet = Array.newBuilder[Edge]

    val inNodes = Array.newBuilder[Node]
    val outNodes = Array.newBuilder[Node]

    for (i <- 0 until n) {
      def newLabel(v: Node) = "%s%s".format(v.label, i)

      val nodeMap = node.map(v => v -> new Node(newLabel(v))).toMap
      val newNodes = node.map(v => nodeMap(v))
      val newEdges = edge.map(e => new Edge(nodeMap(e.src), nodeMap(e.dest)))

      inNodes ++= in.map(v => nodeMap(v))
      outNodes ++= out.map(v => nodeMap(v))
      newNodeSet ++= newNodes
      newEdgeSet ++= newEdges
    }

    new Graph(newNodeSet.result, newEdgeSet.result, inNodes.result, outNodes.result)
  }

  /**
   * Connect two graphs. The outputs of the source and inputs of the destination are connected by round-robbin manner.
   * @param dest
   * @return
   */
  def ->(dest: Graph): Graph = {
    val newNodes = this.node ++ dest.node

    val newEdge = Array.newBuilder[Edge]
    newEdge ++= this.edge

    val max = math.max(this.out.length, dest.in.length)
    for (i <- 0 until max) {
      val outputIndex = i % this.out.length
      val inputIndex = i % dest.in.length
      newEdge += new Edge(this.out(outputIndex), dest.in(inputIndex))
    }

    new Graph(newNodes, newEdge.result, this.in, dest.out)
  }

  /**
   * Create bipartite graph connecting to the destination
   * @param dest
   * @return
   */
  def ->>(dest:Graph): Graph = {
    val newNodes = Array.newBuilder[Node]
    val newEdges = Array.newBuilder[Edge]

    newNodes ++= this.node
    newEdges ++= this.edge

    newNodes ++= dest.node
    newEdges ++= dest.edge

    for(o <- this.out; i <- dest.in) {
      newEdges += new Edge(o, i)
    }

    new Graph(newNodes.result, newEdges.result, this.in, dest.out)
  }

  /**
   * Merge two graphs by removing duplicates
   * @param other
   * @return
   */
  def |(other: Graph): Graph = {
    val nodeWithTheSameLabel = (this.node ++ other.node).groupBy(_.label)
    val nodeMap =
      (for((label, nodes) <- nodeWithTheSameLabel) yield {
         val merged = new Node(nodes.head.label)
         for(each <- nodes) yield each -> merged
      }).flatten.toMap[Node, Node]

    val newNodes = nodeMap.values.toArray
    val newEdges = (this.edge ++ other.edge).map(e => new Edge(nodeMap(e.src), nodeMap(e.dest))).distinct.toArray

    val newInNodes = (this.in ++ other.in).map(v => nodeMap(v))
    val newOutNodes = (this.out ++ other.out).map(v => nodeMap(v))
    new Graph(newNodes, newEdges, newInNodes, newOutNodes)
  }



}


class GraphvizWriter(out: OutputStream, options: Map[String, String]) {

  def this(out: OutputStream) = this(out, Map.empty)

  private val g = new PrintStream(out, true, "UTF-8")
  private var indentLevel = 0

  if (!options.isEmpty) {
    g.println("graph %s".format(toString(options)))
  }

  def digraph(graphName: String = "G")(w: GraphvizWriter => Unit): GraphvizWriter = {
    g.println("digraph %s {".format(graphName))
    indentLevel += 1
    w(this)
    indentLevel -= 1
    g.println("}")
    this
  }

  def newline {
    g.println
  }

  def indent {
    for (i <- 0 until indentLevel)
      g.print(" ")
  }

  private def toString(options: Map[String, String]) =
    "[%s]".format(options.map(p => "%s=%s".format(p._1, p._2)).mkString(", "))

  def node(nodeName: String, options: Map[String, String] = Map.empty): GraphvizWriter = {
    indent
    g.print("\"%s\"".format(nodeName))
    if (!options.isEmpty) {
      g.print(" %s".format(toString(options)))
    }
    g.println(";")
    this
  }

  def arrow(srcNodeID: String, destNodeID: String, options: Map[String, String] = Map.empty): GraphvizWriter = {
    indent
    g.print("\"%s\" -> \"%s\"".format(srcNodeID, destNodeID))
    if (!options.isEmpty) {
      g.print(" %s".format(toString(options)))
    }
    g.println(";")
    this
  }

  def flush = g.flush

  def close = g.close
}

