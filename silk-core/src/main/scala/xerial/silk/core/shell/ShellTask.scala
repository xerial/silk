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
package xerial.silk.core.shell

import java.io.File

import xerial.silk.core.{SilkOp, TaskContext}


case class ShellCommand(context:TaskContext, sc:StringContext, args:Seq[Any]) extends SilkOp[Any] {
  def summary = templateString(sc)

  def name = "ShellCommand"

  private def templateString(sc:StringContext) = {
    sc.parts.mkString("{}")
  }
}

case class ShellEnv(currentDir:File)
