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
package xerial.silk.examples.web_analytics

import xerial.silk._
import xerial.silk.examples.web_analytics.web_analytics_rp.{enriched_page_views, page_views}
import xerial.silk.frame.Frame
import xerial.silk.frame.weaver.TD

object web_analytics {

  class PageViewProcessing[A](input:Frame[A]) {

    def pageviews = input.as[web_analytics_rp.page_views]

    def dedupEvents = pageviews
                      .partitionBy(_.time, _.js_id)
                      .firstsInWindows

    def filterEvents = dedupEvents.filter(_.browser in ("Chrome", "Firefox"))

    def enrichEvents = new Enrichment(filterEvents).resolveCountryName

    def sendToLookerTank = enrichEvents.exportTo("datatank://my-datatank")
  }

  class Enrichment(input:Frame[page_views]) {
    def resolveCountryName = input.mapColumn(_.ip, "TD_IP_TO_COUNTRY('NAME', ?)").selectAll.as[enriched_page_views]
    def referrerCategory = input.mapColumn(_.referrer, "regexp_extract(?, 'https://console.treasruedata.com/([a-z_]+)', 1)")
  }

  // Instantiating workflow DAGs
  val w = new PageViewProcessing(TD("web_analytics_rp").table("page_views"))
  // TODO specify schedule {frequency = ‘daily’, time=‘0:00’, timezone = ‘UTC’, delay_min = 10}
  //w.setSchedule(null) // Need to handle unbounded inputs or incremental processing
  //w.addNotifier{
//    case TASK_FAILURE(e) => // send e-mail to 'alert@your_company.com'
//  }

  // TODO associate workflows and sessions
}