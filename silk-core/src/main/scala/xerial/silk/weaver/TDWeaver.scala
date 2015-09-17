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
package xerial.silk.weaver

import java.util.Properties

import com.treasuredata.client.TDClientConfig
import xerial.core.log.Logger
import xerial.silk._
import xerial.silk.core._

object TDDatabase {

}

case class TDDatabase(databaseName: String) extends Database {

  import scala.collection.JavaConversions._
  def createTable(tableName: String) = NA
  def dropTable(tableName: String) = NA
  def existsTable(tableName: String) = NA
  def listTables: Seq[String] = NA
  def getSchema(tableName: String): TDSchema = NA

}

case class TDSchema(columns: Seq[TDColumn])
case class TDColumn(name: String, dataType: TDType, aliases: Seq[String])

sealed trait TDType
object TDType {
  object INT extends TDType
  object FLOAT extends TDType
  object STRING extends TDType
  case class ARRAY(elementType: TDType) extends TDType
  case class MAP(keyType: TDType, valueType: TDType) extends TDType
}

/**
 *
 */
trait TDConnector {

  def createDatabase(name: String) = NA
  def dropDatabase(name: String) = NA
  def existsDatabase(name: String) = NA
  def listDatabases: Seq[String] = NA
  def openDatabase[U](database: String)(body: TDDatabase => U): U = body(TDDatabase(database))

}

object TDWeaver {

  case class Config(clientConfig:Properties = TDClientConfig.readTDConf())
}

class TDWeaver extends Weaver
                       with StateStore
                       with JDBCWeaver
                       with Logger {
  override type Config = TDWeaver.Config
  override val config: Config = TDWeaver.Config()

  override protected val jdbcDriverName: String = "com.treasuredata.jdbc.TreasureDataDriver"
  override protected def jdbcUrl(databaseName: String): String = s"jdbc:td://api.treasuredata.com/${databaseName}"
  override def jdbcProperties = config.clientConfig

}