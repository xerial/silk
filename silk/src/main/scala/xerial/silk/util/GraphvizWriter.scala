//--------------------------------------
//
// GraphvizWriter.scala
// Since: 2012/12/13 2:02 PM
//
//--------------------------------------

package xerial.silk.util

import java.io.{PrintStream, OutputStream}

/**
 * @author Taro L. Saito
 */
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