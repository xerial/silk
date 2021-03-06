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
package xerial.silk.frame.weaver

import java.util.Properties

import xerial.core.log.Logger
import xerial.silk.frame.Database

object SQLite {
  def memoryDatabase = SQLite(":memory:")
}

case class SQLite(name: String) extends Database {
  override def toString = name
}

object SQLiteWeaver {
  case class Config(jdbcProperties: Properties = new Properties())
}

/**
 *
 */
class SQLiteWeaver extends Weaver with StateStore with JDBCWeaver with Logger {

  import SQLiteWeaver._

  override type Config = SQLiteWeaver.Config
  override val config: Config = Config()

  protected val jdbcDriverName = "org.sqlite.JDBC"
  protected def jdbcUrl(databaseName: String): String = s"jdbc:sqlite:${databaseName}"
  protected def jdbcProperties = config.jdbcProperties

}
