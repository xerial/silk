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
package xerial.silk.macros

import scala.language.existentials

trait OpRef {
  def fullName: String
  def shortName: String
}

/**
 * Useed for asigning user-defined names to a task
 * @param id
 */
case class NamedRef(id: String) extends OpRef {
  def fullName = id
  def shortName = id
}

/**
 * Tells where this task is defined in the source code.
 * @param owner
 * @param name
 * @param source
 * @param line
 * @param column
 */
case class SourceRef(owner: Class[_],
                     name: String,
                     source: String,
                     line: Int,
                     column: Int) extends OpRef {

  def baseTrait: Class[_] = {

    // If the class name contains $anonfun, it is a compiler generated class.
    // If it contains $anon, it is a mixed-in trait
    val isAnonFun = owner.getSimpleName.contains("$anon")
    if (!isAnonFun) {
      owner
    }
    else {
      // If the owner is a mix-in class
      owner.getInterfaces
      .headOption.orElse(Option(owner.getSuperclass))
      .getOrElse(owner)
    }
  }

  private def format(op: Option[String]) : String = op.map(_.toString).getOrElse("")

  def fullName = {
    val className = baseTrait.getSimpleName.replaceAll("\\$", "")
    s"${name}"
  }

  def shortName = {
    Option(name.split("\\.")).map(a => a(a.length - 1)).getOrElse(name)
  }

  override def toString = {
    s"${fullName} [L$line:$column]"
  }

}

