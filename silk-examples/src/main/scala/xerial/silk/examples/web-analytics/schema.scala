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
import xerial.silk.core.Column

object web_analytics_rp {

  trait page_views {
    val time = Column[Long]("time")
    val js_id = Column[String]("js_id")
    val browser = Column[String]("td_browser")
    val ip = Column[String]("td_ip")
    val referrer = Column[String]("td_referrer")
  }
  object page_views extends page_views

  trait enriched_page_views extends page_views {
    val country = Column[String]("country")
  }
  object enriched_page_views extends page_views


}
