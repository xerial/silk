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
package xerial

import xerial.silk.core._
import xerial.silk.core.shell.ShellCommand
import xerial.silk.macros.TaskMacros._

import scala.language.experimental.macros
import scala.language.implicitConversions

/**
 *
 */
package object silk {

  implicit class ToOption[A](v: A) {
    def some = Some[A](v)
  }

  def task[B](block: => B): TaskDef[B] = macro mTaskCommand[B]


  implicit class ShellContext(val sc: StringContext) extends AnyVal {
    def c(args: Any*): ShellCommand = macro mShellCommand
  }

  def NA = {
    val t = new Throwable
    val caller = t.getStackTrace()(1)
    throw NotAvailable(s"${caller.getMethodName} (${caller.getFileName}:${caller.getLineNumber})")
  }


  implicit class SeqToSilk(val s: Seq[Task]) {
    def toSilk: MultipleInputs = macro mToSilk
  }

  implicit def seqToSilk(s: Seq[Task]): MultipleInputs = macro mTaskSeq


}
