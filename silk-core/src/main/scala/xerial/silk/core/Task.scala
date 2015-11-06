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

import xerial.lens.{ConstructorParameter, ObjectSchema}
import xerial.silk.macros.Ref

case class TaskContext(id: Ref, inputs: Seq[Task] = Seq.empty) {
  def addDependencies(others: Seq[Task]): TaskContext = TaskContext(id, inputs ++ others)
}


object Task {
}

trait Task {
  def id: String = context.id.fullName
  def context: TaskContext

  def name: String = this.getClass.getSimpleName
  def description: String


  private def updateLens(paramMap: PartialFunction[ConstructorParameter, AnyRef]): this.type = {
    val sc = ObjectSchema(this.getClass)
    val constructorArgs = sc.constructor.params
    val params = for (p <- constructorArgs) yield {
      paramMap.applyOrElse(p, { p: ConstructorParameter => p.get(this) }).asInstanceOf[AnyRef]
    }
    val c = sc.constructor.newInstance(params.toSeq.toArray[AnyRef])
    c.asInstanceOf[this.type]
  }

  def dependsOn(others: Task*): Task = updateLens { case p if p.name == "context" =>
    p.get(this).asInstanceOf[TaskContext].addDependencies(others)
  }

}

case class TaskDef[A](context: TaskContext)(block: => A)
  extends Task {

  override def id: String = context.id.fullName
  override def name: String = context.id.shortName
  override def description: String = ""

}

case class MultipleInputs(context: TaskContext) extends Task {
  def description = s"${context.inputs.size} inputs"
  override def name = s"MultipleInputs"
}

