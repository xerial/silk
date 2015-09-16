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

import xerial.core.log.Logger
import xerial.msgframe.core.MsgFrame
import xerial.silk.core._
import xerial.silk.core.{Database, DBRef}


/**
 *
 */
trait JDBCExecutor {
  self : Weaver with StateStore with Logger =>

  protected val jdbcDriverName : String

  protected def jdbcUrl(databaseName:String) : String

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

  protected def executeSQL[U](dbRef:DBRef[Database], sql:String) {
    runSQL(dbRef, sql) { rs =>
      // do nothing
    }
  }

  protected def runSQL[U](dbRef:DBRef[Database], sql:String)(handler:ResultSet => U) : U = {
    Class.forName(jdbcDriverName)
    withResource(DriverManager.getConnection(jdbcUrl(dbRef.db.databaseName))) { conn =>
      withResource(conn.createStatement()) { st =>
        info(s"Execute SQL: ${sql}")
        st.execute(sql)
        val rs = st.getResultSet
        handler(rs)
      }
    }
  }

  def eval(silk:SilkOp[_], level:Int = 0) {
    if (!isEvaluated(silk)) {
      val inputs = silk.context.inputs
      info(f"${indent(level)}visit ${silk.name} ${silk.hashCode()}%x (num inputs: ${inputs.size}) : ${silk.summary}")
      // Evaluate parents
      for (in <- inputs) {
        eval(in, level + 1)
      }
      info(f"${indent(level)}evaluate: [${silk.name} ${silk.hashCode()}%x] ${silk.summary}")
      silk match {
        case DBRef(context, db, op) =>
          op match {
            case Create(ifNotExists) =>
            case Drop(ifExists) =>
            case Open =>
          }
        case TableRef(context, dbRef, op, tableName) =>
          op match {
            case Create(ifNotExists) =>
            case Drop(ifExists) => executeSQL(dbRef, s"DROP TABLE${if(ifExists) " IF EXISTS" else ""} ${tableName}")
            case Open =>
          }
        case SQLOp(context, dbRef, sql) =>
          runSQL(dbRef, sql) { rs =>
            val frame = MsgFrame.fromSQL(rs)
            if(frame.numRows > 0) {
              info("frame:\n" + frame)
            }
          }
        case r@RawSQL(context, sc, args) =>
          // TODO resolve db reference from a session?
          val sql = r.toSQL
          Class.forName("org.sqlite.JDBC")
          withResource(DriverManager.getConnection(s"jdbc:sqlite::memory:")) { conn =>
            withResource(conn.createStatement()) { st =>
              st.execute(sql)
              val rs = st.getResultSet
              val frame = MsgFrame.fromSQL(rs)
              info("frame:\n" + frame)
            }
          }
        case MultipleInputs(context) =>
      }
    }

  }


}
