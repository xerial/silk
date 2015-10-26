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
package xerial

import java.io.File
import java.util.Date

import org.joda.time.DateTime
import org.joda.time.format._
import xerial.silk.core._
import xerial.silk.core.shell.ShellCommand
import xerial.silk.macros.SilkMacros._

import scala.language.experimental.macros
import scala.language.implicitConversions

/**
 *
 */
package object silk {

  implicit class ToOption[A](v:A) {
    def some = Some[A](v)
  }

  def task[B](block: => B): TaskDef[B] = macro mTaskCommand[B]

  implicit class SQLContext(val sc: StringContext) extends AnyVal {
    def sql(args: Any*)(implicit db: Database): SQLOp = macro mSQLStr

    private def templateString = {
      sc.parts.mkString("{}")
    }

    def toSQL(args: Any*): String = {
      val b = new StringBuilder
      var i = 0
      for (p <- sc.parts) {
        b.append(p)
        if (i < args.length) {
          b.append(args(i).toString)
        }
        i += 1
      }
      b.result
    }
  }

  implicit class ShellContext(val sc: StringContext) extends AnyVal {
    def c(args: Any*): ShellCommand = macro mShellCommand
  }

  def NA = {
    val t = new Throwable
    val caller = t.getStackTrace()(1)
    throw NotAvailable(s"${caller.getMethodName} (${caller.getFileName}:${caller.getLineNumber})")
  }

  implicit class IntToDuration(n: Int) {
    def hour: Duration = NA
    def day: Duration = Duration(n, Day)
    def month: Duration = NA
    def second: Duration = NA
  }

  implicit class SeqToSilk(val s: Seq[Task]) {
    def toSilk: MultipleInputs = macro mToSilk
  }

  implicit def seqToSilk(s: Seq[Task]) : MultipleInputs = macro mTaskSeq

  def from[A](in: Seq[A]): InputFrame[A] = macro mNewFrame[A]
  def fromFile(in: File): FileInput = macro mFileInput

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

  case class Repeat(duration:Int, unit:DateTimeUnit)
  case class AtSpecificDays(days:Seq[Day])
  case class RecurringSchedule(repeat:Repeat) extends Schedule
  case class FixedSchedule(scheduledTime: DateTime) extends Schedule

  /**
   * Resolve scheduled time of the current context
   */
  case class ScheduledTime(offset: Duration) {
    def +(d: Duration): ScheduledTime = ScheduledTime(d)
    def -(d: Duration): ScheduledTime = ScheduledTime(d)
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
  def everyWeek(n:Int) = Repeat(2, Week)
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
