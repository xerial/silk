/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package xerial.silk.core

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

import xerial.silk.core.util.GraphvizWriter

object TaskGraph {
  def createTaskGraph(leaf: Task): TaskGraph = {

    var numNodes = 0
    val idTable = scala.collection.mutable.Map[Task, Int]()
    val edgeTable = scala.collection.mutable.Set[(Int, Int)]()
    def getId(s: Task) = idTable.getOrElseUpdate(s, {numNodes += 1; numNodes - 1})

    def traverse(s: Task, visited: Set[Int]): Unit = {
      val id = getId(s)
      if (!visited.contains(id)) {
        val updated = visited + id
        for (in <- s.context.inputs) {
          val sourceId = getId(in)
          edgeTable += ((id, sourceId))
          traverse(in, updated)
        }
      }
    }

    traverse(leaf, Set.empty)

    val nodes = idTable.toSeq.sortBy(_._2).map(_._1)
    val edges = for ((id, lst) <- edgeTable.toSeq.groupBy(_._1)) yield id -> lst.map(_._2).sorted
    TaskGraph(nodes, edges)
  }
}

/**
 *
 */
case class TaskGraph(nodes: Seq[Task], dependencies: Map[Int, Seq[Int]]) {

  override def toString = {
    val out = new StringBuilder
    out.append("[nodes]\n")
    for ((n, i) <- nodes.zipWithIndex) {
      out.append(f" [$i:${n.hashCode()}%08x] ${n.context.id.fullName} := [${n.name}] ${n.description}\n")
    }
    out.append("[dependencies]\n")
    for ((n, id) <- nodes.zipWithIndex; dep <- dependencies.get(id)) {
      out.append(s" $id <- ${dep.mkString(", ")}\n")
    }
    out.result()
  }

  def toGraphViz: String = {
    val s = new ByteArrayOutputStream()
    val g = new GraphvizWriter(s, Map("fontname" -> "Roboto"))

    g.digraph("G") { dg =>
      for ((n, id) <- nodes.zipWithIndex) {
        val label = s"${n.context.id.shortName}"
        dg.node(id.toString, Map("label" -> label, "shape" -> "box", "color" -> "\"#5cc2c9\"", "fontcolor" -> "white", "style" -> "filled"))
      }
      for ((srcId, destIdLst: Seq[Int]) <- dependencies; dstId <- destIdLst) {
        dg.arrow(dstId.toString, srcId.toString)
      }
    }
    g.close
    new String(s.toByteArray, StandardCharsets.UTF_8)
  }


}
