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
import xerial.lens.{ConstructorParameter, ObjectBuilder, ObjectSchema}
import xerial.silk.core.util.GraphvizWriter
import xerial.silk.macros.OpRef
import xerial.silk.{Day, DeadLine, Repeat}

case class TaskContext(id: OpRef, config:TaskConfig=TaskConfig(), inputs: Seq[Task]=Seq.empty) {
  def addDependencies(others: Seq[Task]): TaskContext = TaskContext(id, config, inputs ++ others)
  def withConfig(newConfig:TaskConfig) = TaskContext(id, newConfig, inputs)
}


case class TaskConfig(repeat: Option[Repeat] = None,
                      excludeDate: Seq[Day] = Seq.empty,
                      startAt: Option[DateTime] = None,
                      endAt: Option[DateTime] = None,
                      deadline: Option[DeadLine] = None,
                      retry: Int = 3) {

  /**
   * Create a new TaskConfig object with the updated value
   * @param name
   * @param v
   * @tparam V
   * @return
   */
  def set[V](name:String, v:V) : TaskConfig = {
    val params = ObjectSchema.of[TaskConfig].findConstructor.get.params
    val m = (for(p <- params) yield { p.name -> p.get(this) }).toMap[String, Any]
    val b = ObjectBuilder(classOf[TaskConfig])
    for((p, v) <- m) b.set(p, v)
    b.set(name, v)
    b.build
  }

}


object Task {
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

trait Task {
  def id: String = context.id.fullName
  def name: String = this.getClass.getSimpleName
  def summary: String
  def context: TaskContext

  private def updateLens(paramMap:PartialFunction[ConstructorParameter, AnyRef]) : this.type = {
    val sc = ObjectSchema(this.getClass)
    val constructorArgs = sc.constructor.params
    val params = for (p <- constructorArgs) yield {
      paramMap.applyOrElse(p, {p : ConstructorParameter => p.get(this)}).asInstanceOf[AnyRef]
    }
    val c = sc.constructor.newInstance(params.toSeq.toArray[AnyRef])
    c.asInstanceOf[this.type]
  }

  def dependsOn(others: Task*): Task = updateLens { case p if p.name == "context" =>
    p.get(this).asInstanceOf[TaskContext].addDependencies(others)
  }
  def ->(other: Task): Task = other.dependsOn(this)

  def repeat(r: Repeat) = updateConfig("repeat", r)
  def startAt(r: DateTime) = updateConfig("startAt", r)
  def endAt(r: DateTime) = updateConfig("endAt", r)
  def deadline(r: DeadLine) = updateConfig("deadline", r)
  def retry(retryCount:Int) = updateConfig("retry", retryCount)

  private def updateConfig(name:String, value:Any) : this.type = {
    withConfig(context.config.set(name, value))
  }

  private def withConfig(newConfig:TaskConfig): this.type = updateLens { case p if p.name == "context" =>
    p.get(this).asInstanceOf[TaskContext].withConfig(newConfig)
  }

}

case class TaskDef[A](context: TaskContext)(block: => A)
  extends Task {

  override def id: String = context.id.fullName
  override def name: String = context.id.shortName
  override def summary: String = ""

}

case class MultipleInputs(context:TaskContext) extends Task {
  def summary = s"${context.inputs.size} inputs"
  override def name = s"MultipleInputs"
}

case class TaskGraph(nodes: Seq[Task], dependencies: Map[Int, Seq[Int]]) {

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