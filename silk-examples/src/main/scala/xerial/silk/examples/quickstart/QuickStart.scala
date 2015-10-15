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
package xerial.silk.examples.quickstart

import xerial.silk._
import xerial.silk.weaver.TD

/**
 *
 */
object QuickStart {

  implicit def db = TD("presto_production") // .table("query_completion")

  def queryCount(duration:Duration) = sql"""
       SELECT TD_TIME_FORMAT(time, 'yyyy-MM-dd', 'UDT') date,  account_id, count(*) query_count
       FROM query_completion
       WHERE TD_TIME_RANGE(time, '${SCHEDULED_TIME}', '${SCHEDULED_TIME + duration}')
       GROUP BY TD_TIME_FORMAT(time, 'yyyy-MM-dd', 'UDT'), account_id
    """

  //def hourlyCount = queryCount(1.hour).repeat(everyHour)
  //def dailyCount = queryCount(1.day).repeat(everyDay)

  // TODO: save query results to another table.

  // 1. insert into 7 days result into an table, collect them in a single query
  //  Need to avoid duplicate updates

  // 2. aggregate 7 query results and perform single query

  // 3. process this in a single query

  //def weeklyReport = queryCount(last(7.day))





}
