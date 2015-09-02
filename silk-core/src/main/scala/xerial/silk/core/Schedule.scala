package xerial.silk.core

import xerial.silk._

case class RecurringSchedule(since:Option[Schedule], until:Option[Schedule]) extends Schedule
case class FixedSchedule() extends Schedule

trait DateUnit
object Hour extends DateUnit
object Day extends DateUnit
object Week extends DateUnit
object Month extends DateUnit
object Year extends DateUnit

case class Repeat(duration:Int, unit:DateUnit)
case class RepeatInWeek()

class Schedule {
  def +(other:Schedule) : Schedule = null
}
class Duration {

}
/**
 *
 */
object Schedule
{
  implicit class toDate(v:Int) {
    // def hour =
  }

  def everyHour = Repeat(1, Hour)
  def everyDay = Repeat(1, Day)
  def everyWeekDay = NA
  def everyWeek = Repeat(1, Week)
  def everyMonth = Repeat(1, Month)
  def everyYear = Repeat(1, Year)

  def every(d:Day*) = NA

  def endOfHour = NA
  def endOfDay = NA
  def endOfWeek = NA
  def endOfMonth = NA
  def endOfYear = NA

  def lastYear = NA
  def lastMonth = NA
  def lastWeek = NA
  def yesterday = NA
  def today : Schedule = NA
  def tomorrow = NA
  def nextWeek = NA
  def nextMonth = NA
  def nextYear = NA

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
