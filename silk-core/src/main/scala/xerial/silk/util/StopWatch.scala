/*
 * Copyright 2012 Taro L. Saito
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package xerial.silk.util

import java.lang.IllegalStateException
import collection.mutable.{ListBuffer, Stack, LinkedHashMap}


//--------------------------------------
//
// StopWatch.scala
// Since: 2012/01/09 8:31
//
//--------------------------------------

/**
 * @author leo
 */

object PerformanceLogger {
  private val holder = new ThreadLocal[Stack[TimeMeasure]] {
    override def initialValue() = new Stack[TimeMeasure]
  }

  private def contextStack = holder.get()

  private def createNewBlock[A](blockName: String, f: => A): TimeMeasure = new TimeMeasure {
    val name: String = blockName
    def body() = f
  }

  import LogLevel._

  def time[A, B](blockName: String, logLevel: LogLevel = DEBUG, repeat: Int = 1)(f: => A): TimeMeasure = {
    def pushContext(t: TimeMeasure): Unit = contextStack.push(t)
    def popContext: Unit = contextStack.pop

    val m = createNewBlock(blockName, f)
    try {
      pushContext(m)
      m.measure(repeat)
    }
    finally {
      popContext
      reportLog(m, logLevel)
    }
  }

  def block[A](name: String, repeat: Int = 1)(f: => A): TimeMeasure = {
    val m = contextStack.lastOption match {
      case None => throw new IllegalStateException("block {} should be enclosed inside time {}")
      case Some(context) => {
        context.getOrElseUpdate(name, createNewBlock(name, f))
      }
    }
    m.measure(repeat)
  }

  def reportLog(m: TimeMeasure, logLevel: LogLevel): Unit = {
    val l = if (this.isInstanceOf[Logging])
      this.asInstanceOf[Logging].logger
    else
      Logger.getLogger(this.getClass)

    l.log(logLevel)(m.report)
  }
}

trait TimeMeasure extends Ordered[TimeMeasure] {
  val name: String
  def body(): Unit

  private[TimeMeasure] val s = new StopWatch
  private lazy val subMeasure = new LinkedHashMap[String, TimeMeasure]
  private var _executionCount = 0

  private var maxInterval: Double = 0.0
  private var minInterval: Double = Double.MaxValue

  {
    s.stop
    s.reset
  }

  def containsBlock(name: String) = {
    subMeasure.contains(name)
  }

  def apply(name: String): TimeMeasure = {
    subMeasure(name)
  }

  def getOrElseUpdate(name: String, t: => TimeMeasure): TimeMeasure = {
    subMeasure.getOrElseUpdate(name, t)
  }

  def executionCount: Int = _executionCount

  def measure(repeat: Int = 1): TimeMeasure = {
    for (i <- 0 until repeat) {
      s.resume
      try {
        body
      }
      finally {
        val intervalTime = s.stop
        _executionCount += 1

        maxInterval = math.max(maxInterval, intervalTime)
        minInterval = math.min(minInterval, intervalTime)


      }
    }
    this
  }


  def compare(that: TimeMeasure) =
    this.elapsedSeconds.compareTo(that.elapsedSeconds)


  def average: Double = {
    s.getElapsedTime / _executionCount
  }

  def elapsedSeconds : Double = {
    s.getElapsedTime
  }

  def genReportLine: String = {
    "[%s]\ttotal:%.2f sec., count:%,d, avg:%.2f sec., min:%.2f sec., max:%.2f sec.".format(
      name, s.getElapsedTime, executionCount, average, minInterval, maxInterval
    )
  }

  def report: String = {
    def indent(level: Int, s: String): String = {
      (for (i <- 0 until level * 2) yield ' ').mkString + s
    }

    val lines = new ListBuffer[String]
    lines += indent(0, genReportLine)
    for ((k, v) <- subMeasure)
      lines += indent(1, v.genReportLine)

    lines.mkString("\n")
  }

  override def toString: String = report

}


class StopWatch {

  private[StopWatch] object State extends Enumeration {
    val RUNNING, STOPPED = Value
  }

  private var lastSystemTime: Double = System.nanoTime
  private var accumulatedElapsedTime: Double = 0L
  private var state = State.RUNNING

  private val NANO_UNIT: Double = 1000000000L

  /**
   * Gets the elapsed time since this instance is created in seconds.
   *
   * @return the elapsed time in seconds.
   */
  def getElapsedTime: Double = {

    if (state == State.RUNNING) {
      val now = System.nanoTime()
      val diff = now - lastSystemTime
      (accumulatedElapsedTime + diff) / NANO_UNIT
    }
    else {
      accumulatedElapsedTime / NANO_UNIT
    }
  }

  /**
   * Reset the stop watch. The subsequent calls to
   * getElapsedTime or getIntervalTiem will measure the time interval
   * beginning from this method call.
   */
  def reset = {
    lastSystemTime = System.nanoTime()
    accumulatedElapsedTime = 0L
  }

  /**
   * Stop the timer
   * @return interval time since the last resume call
   */
  def stop: Double = {
    if (state == State.STOPPED)
      return 0.0

    // elapsed time
    val now = System.nanoTime()
    val diff = now - lastSystemTime
    accumulatedElapsedTime += diff
    lastSystemTime = now;

    state = State.STOPPED;
    diff / NANO_UNIT
  }

  def resume {
    if (state == State.RUNNING)
      return

    lastSystemTime = System.nanoTime();
    state = State.RUNNING;
  }

  def reportElapsedTime: String = {
    "%.2f sec." format (getElapsedTime)
  }


}


