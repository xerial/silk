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
package xerial.silk.core

/**
 *
 */
class ScheduleTest extends SilkSpec {

  import xerial.silk._

  "schedule" should {

    "have support to convert String to DateTime" in {

      "2001-02-03 04:05:06 UTC".toDateTime
      // TODO TimeZone support. jodatime acceplts only UTC
      //"2001-02-03 04:05:06 JST".toDateTime
      //"2001-02-03 04:05:06 UDT".toDateTime
      "2001-02-03 04:05:06".toDateTime
      "2001-02-03 04:05".toDateTime
      "2001-02-03 04".toDateTime
      "2001-02-03".toDateTime
    }

    "support repetition" in {

      val myTask = task {
        "hello"
      }.repeat(everyHour)
                   .startAt(today)


    }
  }
}
