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

import xerial.silk.core.{TaskGraph, SilkSpec, Task}

object SQLiteWeaverTest {


}

/**
 *
 */
class SQLiteWeaverTest extends SilkSpec {

  import xerial.silk._

  "SQLiteWeaver" should {
    "run SQL query" in {

      val db = SQLite("target/sample.db")
      val select = db.sql("select 1")

      info(select)
      val g = TaskGraph.createTaskGraph(select)
      info(g)

      val w = new SQLiteWeaver
      w.weave(select)

      val select2 = db.sql("select 2")
      w.weave(select2)
    }

    "run pipeline query" in {
      val db = SQLite("target/sample2.db")
      val drop = db.dropTableIfExists("t")
      val table = drop -> db.sql("create table if not exists t (id integer, name string)")
      val insert = for (i <- 0 until 3) yield {
        table -> db.sql(s"insert into t values(${i}, 'leo')")
      }
      val populate = insert.toSilk
      val selectAll = populate -> db.sql("select * from t")
      info(selectAll)
      val g = TaskGraph.createTaskGraph(selectAll)
      info(g)

      val w = new SQLiteWeaver()
      w.weave(selectAll)
    }

    "sql-only workflows" taggedAs ("sql") in {

      class W(dbName: String) {
        val db = SQLite(s"target/${dbName}")
        val cleanUp = db.dropTableIfExists("t")
        val t1 = db.sql("create table if not exists t (id integer, name string)") dependsOn cleanUp
        val ins1 = db.sql("insert into t values(1, 'leo')") dependsOn t1
        val ins2 = db.sql("insert into t values(2, 'yui')") dependsOn t1
        val select = db.sql("select * from t") dependsOn(ins1, ins2)
      }

      val myworkflow = new W("sample4")
      info(myworkflow.select)

      val g = TaskGraph.createTaskGraph(myworkflow.select)
      info(g)

      val w = new SQLiteWeaver
      w.weave(myworkflow.select)

    }

  }
}
