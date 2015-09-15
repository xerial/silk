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

import xerial.lens.ObjectSchema

/**
 * Base trait for DAG operations
 */
trait SilkOp[A] {
  def context: FContext
  def inputs: Seq[SilkOp[_]]
  def summary: String
  def name: String

  def dependsOn(others: SilkOp[_]*): SilkOp[A] = {
    val sc = ObjectSchema(this.getClass)
    val constructorArgs = sc.constructor.params
    val hasInputsColumn = constructorArgs.find(_.name == "inputs").isDefined
    val params = for (p <- constructorArgs) yield {
      val newDependencies = this.inputs ++ others
      val newV = if (p.name == "inputs") {
        newDependencies
      }
      else {
        p.get(this)
      }
      newV.asInstanceOf[AnyRef]
    }
    if (hasInputsColumn) {
      val c = sc.constructor.newInstance(params.toSeq.toArray[AnyRef])
      c.asInstanceOf[this.type]
    }
    else {
      Knot[A](this.inputs ++ others, this)
    }
  }

  def ->(other: SilkOp[A]): SilkOp[A] = other.dependsOn(this)

}

object SilkOp {

  def createOpGraph(leaf:SilkOp[_]): OpGraph = {

    var numNodes = 0
    val idTable = scala.collection.mutable.Map[SilkOp[_], Int]()
    val edgeTable = scala.collection.mutable.Set[(Int, Int)]()
    def getId[A](s:SilkOp[A]) = idTable.getOrElseUpdate(s, {numNodes += 1; numNodes-1})

    def traverse(s:SilkOp[_], visited:Set[Int]): Unit = {
      val id = getId(s)
      if(!visited.contains(id)){
        val updated = visited + id
        for(in <- s.inputs) {
          val sourceId = getId(in)
          edgeTable += ((id, sourceId))
          traverse(in, updated)
        }
      }
    }

    traverse(leaf, Set.empty)

    val nodes = idTable.toSeq.sortBy(_._2).map(_._1)
    val edges = for((id, lst) <- edgeTable.toSeq.groupBy(_._1)) yield id -> lst.map(_._2).sorted
    OpGraph(nodes, edges)
  }


}


case class OpGraph(nodes:Seq[SilkOp[_]], dependencies:Map[Int, Seq[Int]]) {

  override def toString = {
    val out = new StringBuilder
    out.append("[nodes]\n")
    for((n, i) <- nodes.zipWithIndex) {
      out.append(f" [$i:${n.hashCode()}%08x] ${n.context.targetName} := [${n.name}] ${n.summary}\n")
    }
    out.append("[dependencies]\n")
    for((n, id) <- nodes.zipWithIndex; dep <- dependencies.get(id)) {
      out.append(s" $id <- ${dep.mkString(", ")}\n")
    }
    out.result()
  }

}