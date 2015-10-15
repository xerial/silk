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

import org.joda.time.DateTime
import xerial.lens.{ObjectBuilder, ObjectSchema}
import xerial.silk.core.util.GraphvizWriter
import xerial.silk.macros.OpRef
import xerial.silk.{Day, DeadLine, Repeat}

case class TaskContext(id: OpRef, inputs: Seq[SilkOp[_]]) {
  def addDependencies(others: Seq[SilkOp[_]]): TaskContext = TaskContext(id, inputs ++ others)
}

object TaskContext {
  def apply(id: OpRef, input: SilkOp[_]): TaskContext = TaskContext(id, Seq(input))
  def apply(id: OpRef): TaskContext = TaskContext(id, Seq.empty)
}


trait Task {
  def id: String
  def context: TaskContext
}

case class TaskConfig(repeat: Option[Repeat] = None,
                      excludeDate: Seq[Day] = Seq.empty,
                      startAt: Option[DateTime] = None,
                      endAt: Option[DateTime] = None,
                      deadline: Option[DeadLine] = None,
                      retry: Int = 3) {

  def set[V](name:String, v:V) : TaskConfig = {
    val params = ObjectSchema.of[TaskConfig].findConstructor.get.params
    val m = (for(p <- params) yield { p.name -> p.get(this) }).toMap[String, AnyVal]
    val b = ObjectBuilder(classOf[TaskConfig])
    for((p, v) <- m) b.set(p, v)
    b.set(name, v)
    b.build
  }

}

case class TaskDef[A](context: TaskContext, config:TaskConfig)
                     (block: => A)
  extends Task {

  override def id: String = context.id.fullName
  def repeat(r: Repeat) = TaskDef(context, config.set("repeat", r))(block)
  def startAt(r: DateTime) = TaskDef(context, config.set("startAt", r))(block)
  def endAt(r: DateTime) = TaskDef(context, config.set("endAt", r))(block)
  def deadline(r: DeadLine) = TaskDef(context, config.set("deadline", r))(block)
  def retry(retryCount:Int) = TaskDef(context, config.set("retry", retryCount))(block)
}


/**
 * Base trait for DAG operations
 */
trait SilkOp[A] extends Task {
  def id = context.id.fullName
  def context: TaskContext
  def summary: String
  def name: String

  def dependsOn(others: SilkOp[_]*): SilkOp[A] = {
    val sc = ObjectSchema(this.getClass)
    val constructorArgs = sc.constructor.params
    val hasInputsColumn = constructorArgs.find(_.name == "context").isDefined
    val params = for (p <- constructorArgs) yield {
      val newV = if (p.name == "context") {
        p.get(this).asInstanceOf[TaskContext].addDependencies(others)
      }
      else {
        p.get(this)
      }
      newV.asInstanceOf[AnyRef]
    }
    val c = sc.constructor.newInstance(params.toSeq.toArray[AnyRef])
    c.asInstanceOf[this.type]
  }

  def ->(other: SilkOp[A]): SilkOp[A] = other.dependsOn(this)

  override def toString = summary
}


object SilkOp {

  def createOpGraph(leaf: SilkOp[_]): OpGraph = {

    var numNodes = 0
    val idTable = scala.collection.mutable.Map[SilkOp[_], Int]()
    val edgeTable = scala.collection.mutable.Set[(Int, Int)]()
    def getId[A](s: SilkOp[A]) = idTable.getOrElseUpdate(s, {numNodes += 1; numNodes - 1})

    def traverse(s: SilkOp[_], visited: Set[Int]): Unit = {
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
    OpGraph(nodes, edges)
  }

}


case class OpGraph(nodes: Seq[SilkOp[_]], dependencies: Map[Int, Seq[Int]]) {

  override def toString = {
    val out = new StringBuilder
    out.append("[nodes]\n")
    for ((n, i) <- nodes.zipWithIndex) {
      out.append(f" [$i:${n.hashCode()}%08x] ${n.context.id.fullName} := [${n.name}] ${n.summary}\n")
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