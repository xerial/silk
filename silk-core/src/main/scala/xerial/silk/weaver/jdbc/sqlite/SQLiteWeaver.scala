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
import xerial.silk.core.{SilkOp, FContext}
import xerial.silk.core.sql._
import xerial.silk.weaver.{SequentialOptimizer, StaticOptimizer, Weaver}

case class SQLite(databaseName: String) extends Database {
  override def toString = databaseName
}

object SQLite {

  import FrameMacros._

  def open(path: String): DBRef[SQLite] = macro mOpen
  def create(path: String): DBRef[SQLite] = macro mCreate
  def delete(path: String): DBRef[SQLite] = macro mDelete
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


  def weave[A](frame: Frame[A]): Unit = {
    debug(s"frame:\n${frame}")

    // TODO inject optimizer
    val optimizer = new SequentialOptimizer(Seq.empty)
    val optimized = optimizer.transform(frame)
    debug(s"optimized frame:\n${optimized}")

    eval(frame)

  }


  def eval(silk:SilkOp) {
    // Evaluate parents
    for(in <- silk.inputs) {
      eval(in)
    }
    info(s"evaluate: ${silk.summary}")
    silk match {
      case DBRef(fc, db, op) =>
        op match {
          case Create(ifNotExists)=>
          case Drop(ifExists) =>
          case Open =>
        }
      case TableRef(fc, dbRef, op, tableName) =>
        op match {
          case Create(ifNotExists) =>
          case Drop(ifExists) =>
          case Open =>
        }
      case SQLOp(fc, dbRef, sql) =>
        Class.forName("org.sqlite.JDBC")
        withResource(DriverManager.getConnection(s"jdbc:sqlite:${dbRef.db.databaseName}")) { conn =>
          withResource(conn.createStatement()) { st =>
            st.execute(sql)
            st.getResultSet
          }
        }
      case r@RawSQL(fc, sc, args) =>
        val sql = r.toSQL
        info(sql)
      case Knot(fc, inputs, output) =>
        eval(output)
    }
  }



}
