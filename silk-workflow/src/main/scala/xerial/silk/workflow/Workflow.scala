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
package xerial.silk.workflow

import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormatterBuilder, DateTimeFormat, ISODateTimeFormat}
import xerial.lens.{ObjectBuilder, ObjectSchema}
import xerial.silk._
import xerial.silk.workflow.Workflow.{DeadLine, Day, Repeat}


case class TaskConfig(repeat: Option[Repeat] = None,
                      excludeDate: Seq[Day] = Seq.empty,
                      startAt: Option[DateTime] = None,
                      endAt: Option[DateTime] = None,
                      deadline: Option[DeadLine] = None,
                      retry: Int = 3) {

  /**
   * Create a new TaskConfig object with the updated value
   * @param name
   * @param v
   * @tparam V
   * @return
   */
  def set[V](name: String, v: V): TaskConfig = {
    val params = ObjectSchema.of[TaskConfig].findConstructor.get.params
    val m = (for (p <- params) yield {p.name -> p.get(this)}).toMap[String, Any]
    val b = ObjectBuilder(classOf[TaskConfig])
    for ((p, v) <- m) b.set(p, v)
    b.set(name, v)
    b.build
  }

  /*
    def repeat(r: Repeat) = updateConfig("repeat", r)
  def startAt(r: DateTime) = updateConfig("startAt", r)
  def endAt(r: DateTime) = updateConfig("endAt", r)
  def deadline(r: DeadLine) = updateConfig("deadline", r)
  def retry(retryCount: Int) = updateConfig("retry", retryCount)

  private def updateConfig(name: String, value: Any): this.type = {
    withConfig(context.config.set(name, value))
  }

  private def withConfig(newConfig: TaskConfig): this.type = updateLens { case p if p.name == "context" =>
    p.get(this).asInstanceOf[TaskContext].withConfig(newConfig)
  }


   */
}


/**
 *
 */
object Workflow {

  implicit class IntToDuration(n: Int) {
    def hour: Duration = NA
    def day: Duration = Duration(n, Day)
    def month: Duration = NA
    def second: Duration = NA
  }

  trait TaskVariable

  def SCHEDULED_TIME: ScheduledTime = NA

  val defaultDateTimeFormatter = {
    val parsers = Array(
      ISODateTimeFormat.dateTimeParser(),
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss z"),
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"),
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm"),
      DateTimeFormat.forPattern("yyyy-MM-dd HH"),
      DateTimeFormat.forPattern("yyyy-MM-dd")
    )
    new DateTimeFormatterBuilder()
    .append(null, parsers.map(_.getParser))
    .toFormatter
  }

  // Scheduling constants
  implicit class StringToDateTimeConverter(dateStr: String) {
    def toDateTime: DateTime = convertToDateTime(dateStr)
  }

  def convertToDateTime(dateTimeStr: String): DateTime = {
    defaultDateTimeFormatter.parseDateTime(dateTimeStr)
  }

  sealed trait DateTimeUnit
  object Second extends DateTimeUnit
  object Minute extends DateTimeUnit
  object Hour extends DateTimeUnit
  object Day extends DateTimeUnit
  object Week extends DateTimeUnit
  object Month extends DateTimeUnit
  object Year extends DateTimeUnit

  case class Repeat(duration: Int, unit: DateTimeUnit)
  case class AtSpecificDays(days: Seq[Day])
  case class RecurringSchedule(repeat: Repeat) extends Schedule
  case class FixedSchedule(scheduledTime: DateTime) extends Schedule

  /**
   * Resolve scheduled time of the current context
   */
  case class ScheduledTime(offset: Duration) {
    def +(d: Duration): ScheduledTime = ScheduledTime(d)
    def -(d: Duration): ScheduledTime = ScheduledTime(d)
    def toUnixTime : Long = 0L // TODO
  }


  class Schedule {
    def +(other: Schedule): Schedule = null
  }
  case class Duration(duration: Int, unit: DateTimeUnit)
  trait DeadLine


  def everyHour = Repeat(1, Hour)
  def everyDay = Repeat(1, Day)
  def everyWeekDay = NA
  def everyWeek = Repeat(1, Week)
  def everyWeek(n: Int) = Repeat(2, Week)
  def everyMonth = Repeat(1, Month)
  def everyYear = Repeat(1, Year)

  def every(d: Day*) = NA

  def endOfHour = NA
  def endOfDay = NA
  def endOfWeek = NA
  def endOfMonth = NA
  def endOfYear = NA

  def lastYear = NA
  def lastMonth = NA
  def lastWeek = NA
  def last(d: Duration) = NA
  def yesterday = today.minusDays(1)
  def now = DateTime.now()
  def today = DateTime.now.toLocalDate.toDateTimeAtCurrentTime
  def tomorrow = today.plusDays(1)
  def nextWeek = NA
  def nextMonth = NA
  def nextYear = NA


  def midnight = NA
  def evening = NA
  def until(scheduleTime: ScheduledTime) = NA


  sealed trait CalendarDate

  sealed trait Day extends CalendarDate
  object Monday extends Day
  object Tuesday extends Day
  object Wednesday extends Day
  object Thursday extends Day
  object Friday extends Day
  object Saturday extends Day
  object Sunday extends Day

  sealed trait Month extends CalendarDate
  object January extends Month
  object February extends Month
  object March extends Month
  object April extends Month
  object May extends Month
  object June extends Month
  object July extends Month
  object August extends Month
  object September extends Month
  object October extends Month
  object November extends Month
  object December extends Month

}
