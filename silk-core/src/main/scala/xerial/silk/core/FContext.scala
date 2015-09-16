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


trait TaskId {
  def targetName : String
}

case class NameId(id:String) extends TaskId {
  def targetName = id
}

/**
 * Context of the function definition
 * @param owner
 * @param name
 * @param localValName
 * @param parentValName
 * @param source
 * @param line
 * @param column
 */
case class SourceLoc(owner: Class[_],
                     name: String,
                     localValName: Option[String],
                     parentValName: Option[String],
                     source: String,
                     line: Int,
                     column: Int) extends TaskId {

  def baseTrait: Class[_] = {

    // If the class name contains $anonfun, it is a compiler generated class.
    // If it contains $anon, it is a mixed-in trait
    val isAnonFun = owner.getSimpleName.contains("$anon")
    if (!isAnonFun) {
      owner
    }
    else {
      // If the owner is a mix-in class
      owner.getInterfaces.headOption orElse
        Option(owner.getSuperclass) getOrElse
        owner
    }
  }

  private def format(op: Option[String]) = {
    if (op.isDefined) {
      s"${op.get}:"
    }
    else {
      ""
    }
  }

  def targetName = {
    val className = baseTrait.getSimpleName.replaceAll("\\$", "")
    s"${className}:${name}"
//    val method = if (name == "<constructor>") "" else s"${name}"
//    val v = localValName.filter(_ != name).map(lv => s"${lv}").getOrElse(method)
//    s"${className}:${v}"
  }

  override def toString = {
    val className = baseTrait.getSimpleName.replaceAll("\\$", "")
    val method = if (name == "<constructor>") "" else s".${name}"
    val targetName = localValName.filter(_ != name).map(lv => s"${method}:${lv}").getOrElse(method)
    val parentStr = if (parentValName.isDefined) s" (parent:${parentValName}) " else ""
    s"${className}${targetName}${parentStr} [L$line:$column]"
  }

  def refID: String = {
    s"${owner.getName}:${format(localValName)}${format(parentValName)}$name"
  }
}

