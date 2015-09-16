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

import java.sql.{ResultSet, DriverManager, Driver, Connection}

import xerial.core.log.Logger
import xerial.msgframe.core.MsgFrame
import xerial.silk.core._
import xerial.silk.weaver.{SequentialOptimizer, StaticOptimizer, Weaver}

case class SQLite(databaseName: String) extends Database {
  override def toString = databaseName
}

object SQLite {

  import FrameMacros._

  def openDatabase(path: String): DBRef[SQLite] = macro mOpen
  def createDatabase(path: String): DBRef[SQLite] = macro mCreate
  def deleteDatabase(path: String): DBRef[SQLite] = macro mDelete
}


object SQLiteWeaver {
  case class Config()

  def withResource[A <: AutoCloseable, U](in:A)(body: A=>U) : U = {
    try {
      body(in)
    }
    finally {
      if(in != null) {
        in.close()
      }
    }
  }
}


/**
 *
 */
class SQLiteWeaver extends Weaver with Logger {

  import SQLiteWeaver._

  override type Config = SQLiteWeaver.Config
  override val config: Config = Config()

  private val evaluatedMark = collection.mutable.Set[SilkOp[_]]()

  def weave[A](op: SilkOp[A]): Unit = {
    debug(s"frame:\n${op}")

    // TODO inject optimizer
    val optimizer = new SequentialOptimizer(Seq.empty)
    val optimized = optimizer.transform(op)
    debug(s"optimized frame:\n${optimized}")

    eval(optimized)
  }

  private def executeSQL[U](dbRef:DBRef[Database], sql:String) {
    runSQL(dbRef, sql) { rs =>
      // do nothing
    }
  }

  private def runSQL[U](dbRef:DBRef[Database], sql:String)(handler:ResultSet => U) : U = {
    Class.forName("org.sqlite.JDBC")
    withResource(DriverManager.getConnection(s"jdbc:sqlite:${dbRef.db.databaseName}")) { conn =>
      withResource(conn.createStatement()) { st =>
        info(s"Execute SQL: ${sql}")
        st.execute(sql)
        val rs = st.getResultSet
        handler(rs)
      }
    }
  }

  private def indent(level:Int) : String = {
    if(level > 0)
      (0 until level).map(i => " ").mkString
    else
      ""
  }

  def eval(silk:SilkOp[_], level:Int = 0) {

    if (!evaluatedMark.contains(silk)) {
      evaluatedMark += silk
      info(f"${indent(level)}visit ${silk.name} ${silk.hashCode()}%x (num inputs: ${silk.inputs.size}) : ${silk.summary}")
      // Evaluate parents
      for (in <- silk.inputs) {
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
          // TODO resolve db reference
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
