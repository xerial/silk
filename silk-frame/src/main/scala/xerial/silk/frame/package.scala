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
package xerial.silk

import java.io.File

import xerial.silk.frame.Database
import xerial.silk.macros.TaskMacros._

/**
 *
 */
package object frame {

  implicit class SQLContext(val sc: StringContext) extends AnyVal {
    def sql(args: Any*)(implicit db: Database): SQLOp = macro mSQLStr

    private def templateString = {
      sc.parts.mkString("{}")
    }

    def toSQL(args: Any*): String = {
      val b = new StringBuilder
      var i = 0
      for (p <- sc.parts) {
        b.append(p)
        if (i < args.length) {
          b.append(args(i).toString)
        }
        i += 1
      }
      b.result
    }
  }


  def from[A](in: Seq[A]): InputFrame[A] = macro mNewFrame[A]
  def fromFile(in: File): FileInput = macro mFileInput

}
