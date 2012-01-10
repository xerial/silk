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

import collection.mutable.{Stack, HashMap, LinkedHashMap}
import java.lang.IllegalStateException
import xerial.silk.core.{Logger, LogLevel, Logging}


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

  private def createNewBlock[A](blockName:String, f: =>A) : TimeMeasure = new TimeMeasure {
    val name : String = blockName
    def body() = f
  }

  import LogLevel._
  def time[A](blockName:String, logLevel:LogLevel = DEBUG)(f: => A) : TimeMeasure = {
    def pushContext(t:TimeMeasure) : Unit =  contextStack.push(t)
    def popContext : Unit = contextStack.pop

    val m =createNewBlock(blockName, f)
    try {
      pushContext(m)
      m.measure
    }
    finally {
      popContext
      reportLog(m, logLevel)
    }
  }
  
  def block[A](name:String)(f: => A) : TimeMeasure = {
    val m = contextStack.lastOption match {
      case None => throw new IllegalStateException("block {} should be enclosed inside time {}")
      case Some(context) => {
        context.getOrElseUpdate(name, createNewBlock(name, f))
      }
    }
    m.measure
  }
  
  def reportLog(m:TimeMeasure, logLevel:LogLevel) : Unit = {
    if(this.isInstanceOf[Logging])
      this.asInstanceOf[Logging].log(logLevel)(m.report)
    else
      Logger.getLogger(this.getClass).log(logLevel)(m.report)
  }
}

trait TimeMeasure {
  val name: String
  def body(): Unit

  private[TimeMeasure] val s = new StopWatch
  private lazy val subMeasure = new LinkedHashMap[String, TimeMeasure]
  private var _executionCount = 0

  {
    s.stop
    s.resume
  }

  def containsBlock(name:String) = {
    subMeasure.contains(name)
  }
  
  def apply(name:String) : TimeMeasure = {
    subMeasure(name)
  }
  
  def getOrElseUpdate(name:String, t: => TimeMeasure) : TimeMeasure = {
    subMeasure.getOrElseUpdate(name, t)
  }

  def executionCount : Int = _executionCount

  def measure: TimeMeasure = {
    s.resume
    try {
      body
    }
    finally {
      s.stop
      _executionCount += 1
    }
    this
  }

/*
  private def time(fun: => Unit) : TimeMeasure = {
    time("block" + (subMeasure.size + 1).toString)(fun)
  }

  private def time(blockName: String)(fun: => Unit) : TimeMeasure = {
    val m: TimeMeasure = subMeasure.getOrElseUpdate(blockName,
      new TimeMeasure {
        val name : String = blockName
        def body() = fun
      }
    )
    m.measure
  }
*/

  def elapsedTime: Double = {
    s.getElapsedTime
  }

  def report : String = {
    val top = "-%s: %.2f sec.".format(name, s.getElapsedTime)
    val next = for((k, v) <- subMeasure)
      yield " -%s: %.2f sec".format(k, v.s.getElapsedTime)

    top + "\n" + next.mkString("\n")
  }

  override def toString : String = report

}



class StopWatch {

  private[StopWatch] object State extends Enumeration {
    val RUNNING, STOPPED = Value
  }

  private var lastSystemTime: Double = System.nanoTime
  private var accumulatedElapsedTime: Double = 0L
  private var state = State.RUNNING

  /**
   * Gets the elapsed time since this instance is created in seconds.
   *
   * @return the elapsed time in seconds.
   */
  def getElapsedTime: Double = {
    val NANO_UNIT: Double = 1000000000L
    if (state == State.RUNNING) {
      val now = System.nanoTime()
      val diff = now - lastSystemTime
      return (accumulatedElapsedTime + diff) / NANO_UNIT;
    }
    else {
      return accumulatedElapsedTime / NANO_UNIT;
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

  def stop {
    if (state == State.STOPPED)
      return

    // elapsed time
    def now = System.nanoTime();
    accumulatedElapsedTime += now - lastSystemTime;
    lastSystemTime = now;

    state = State.STOPPED;
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


