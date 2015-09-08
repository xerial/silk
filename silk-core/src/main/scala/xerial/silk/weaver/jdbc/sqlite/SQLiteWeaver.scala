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
package xerial.silk.weaver.jdbc.sqlite

import java.sql.{DriverManager, Driver, Connection}

import xerial.core.log.Logger
import xerial.silk.core.FContext
import xerial.silk.core.sql.{DBRef, Frame, FrameMacros}
import xerial.silk.weaver.{StaticOptimizer, Weaver}

case class SQLite(path: String) {
  override def toString = path
}

object SQLite {

  import FrameMacros._

  def open(path: String): DBRef[SQLite] = macro mOpen
  def create(path: String): DBRef[SQLite] = macro mCreate
  def delete(path: String): DBRef[SQLite] = macro mDelete
}

case class SQLiteTable(context: FContext, path: String, table: String) extends Frame[Any] {
  override def summary: String = s"$path.$table"
  override def inputs: Seq[Frame[_]] = Seq.empty
}

object SQLiteWeaver {

  case class Config()

}

/**
 *
 */
class SQLiteWeaver extends Weaver with Logger {

  import SQLiteWeaver._

  override type Config = SQLiteWeaver.Config
  override val config: Config = Config()

  def weave[A](frame: Frame[A]): Unit = {
    debug(s"frame:\n${frame}")

    val optimizer = new StaticOptimizer(Seq.empty)
    val optimized = optimizer.optimize(frame)
    debug(s"optimized frame:\n${optimized}")



  }

}
