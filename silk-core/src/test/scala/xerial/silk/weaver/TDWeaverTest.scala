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

import xerial.silk.core._

/**
 *
 */
class TDWeaverTest extends SilkSpec {

  "TDWeaver" should {

    "submit TD queries" in {
      val db = TD("sample_datasets")
      val count = db.sql("select count(*) from www_access")
      val head = db.sql("select * from www_access limit 3") dependsOn count

      val g = Task.createTaskGraph(head)
      val dot = g.toGraphViz
      info(dot)

      val w = new TDWeaver()
      w.weave(head)
    }

    case class HourlySummary(time: Long, cnt: Int)

    "scehdule TD queries" in {
      val db = TD("sample_datasets")
      val nasdaq = db.table("nasdaq")

      val targetTable = TD("leodb").createTableIfNotExists("weaver_hourly_sumary", "")
      //val hourlySummary = db.insertInto(targetTable, null)

      //      db.sql(
      //        s"""insert into ${targetTable} select ${scheduledTime}, count(*) c
      //                                                                |from www_access
      //                                                                |where TD_TIME_RANGE(time, ${scheduledTime}, ${scheduledTime})""".stripMargin)

      //val dailySummary = db.sql(s"select avg(c) from ")
    }


  }
}
