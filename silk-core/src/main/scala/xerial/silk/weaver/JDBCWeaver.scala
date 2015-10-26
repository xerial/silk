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

import java.sql.{DriverManager, ResultSet}
import java.util.Properties

import xerial.core.log.Logger
import xerial.msgframe.core.MsgFrame
import xerial.silk.core._
import xerial.silk.core.Database


/**
 *
 */
trait JDBCWeaver {
  self : Weaver with StateStore with Logger =>

  protected val jdbcDriverName : String
  protected def jdbcUrl(databaseName:String) : String
  protected def jdbcProperties : Properties

  protected def withResource[A <: AutoCloseable, U](in:A)(body: A=>U) : U = {
    try {
      body(in)
    }
    finally {
      if(in != null) {
        in.close()
      }
    }
  }

  protected def executeSQL[U](db:Database, sql:String) {
    runSQL(db, sql) { rs =>
      // do nothing
    }
  }

  protected def runSQL[U](db:Database, sql:String)(handler:ResultSet => U) : U = {
    Class.forName(jdbcDriverName)
    withResource(DriverManager.getConnection(jdbcUrl(db.name), jdbcProperties)) { conn =>
      withResource(conn.createStatement()) { st =>
        info(s"Execute SQL: ${sql}")
        st.execute(sql)
        val rs = st.getResultSet
        handler(rs)
      }
    }
  }

  def eval(silk:Task, level:Int = 0) {
    if (isEvaluated(silk)) {
      trace(f"${indent(level)}skip  ${silk.context.id} ${silk.name} ${silk.hashCode()}%x : ${silk.summary}")
    }
    else {
      val inputs = silk.context.inputs
      debug(f"${indent(level)}visit ${silk.context.id} ${silk.name} ${silk.hashCode()}%x (num inputs: ${inputs.size}) : ${silk.summary}")
      // Evaluate parents
      for (in <- inputs) {
        eval(in, level + 1)
      }
      debug(f"${indent(level)}evaluate: [${silk.name} ${silk.hashCode()}%x] ${silk.summary}")
      silk match {
        case OpenTable(context, db, tableName) =>
          // do nothing
        case CreateTable(context, db, tableName, colDef) =>
          executeSQL(db, s"CREATE TABLE ${tableName}($colDef)")
        case CreateTableIfNotExists(context, db, tableName, colDef) =>
          executeSQL(db, s"CREATE TABLE IF NOT EXISTS ${tableName}($colDef)")
        case DropTable(context, db, tableName) =>
          executeSQL(db, s"DROP TABLE ${tableName}")
        case DropTableIfExists(context, db, tableName) =>
          executeSQL(db, s"DROP TABLE IF EXISTS ${tableName}")
        case SQLOp(context, dbRef, sql) =>
          runSQL(dbRef, sql) { rs =>
            val frame = MsgFrame.fromSQL(rs)
            if(frame.numRows > 0) {
              info("frame:\n" + frame)
            }
          }
        case MultipleInputs(context) =>
        case SelectAll(context, table) =>
          val sql = s"SELECT * FROM ${table.tableName}"
          runSQL(table.db, sql) { rs =>
            val frame = MsgFrame.fromSQL(rs)
            if(frame.numRows > 0) {
              info("frame:\n" + frame)
            }
          }
      }
    }

  }


}
