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


//--------------------------------------
//
// StopWatch.scala
// Since: 2012/01/09 8:31
//
//--------------------------------------

/**
 * @author leo
 */
object StopWatch {

  def time(f: => Unit) : TimeMeasure = {
    time("main")(f)
  }

  private val holder = new ThreadLocal[Stack[TimeMeasure]] {
    override def initialValue() = new Stack[TimeMeasure]
  }

  def time(blockName:String)(f: => Unit) : TimeMeasure = {
    val contextStack = holder.get()
    def getContextTimeMeasure : Option[TimeMeasure] = contextStack.lastOption
    def pushContext(t:TimeMeasure) : Unit =  contextStack.push(t)
    def popContext : Unit = contextStack.pop
    def newMeasure : TimeMeasure =  new TimeMeasure {
      val name : String = blockName
      def body() = f
    }

    val m = getContextTimeMeasure match {
      case None => newMeasure
      case Some(parent) => parent.getOrElseUpdate(f, newMeasure)
    }
    try {
      pushContext(m)
      m.measure
    }
    finally {
      popContext
    }
  }



}

trait TimeMeasure {
  val name: String
  def body(): Unit

  private[TimeMeasure] val s = new StopWatch
  private lazy val subMeasure = new LinkedHashMap[String, TimeMeasure]

  {
    s.stop
    s.resume
  }

  def getOrElseUpdate(fun: => Unit, t: => TimeMeasure) : TimeMeasure = {
    subMeasure.getOrElseUpdate(fun.hashCode().toString, t)
  }

  def measure: TimeMeasure = {
    s.resume
    try {
      body
    }
    finally {
      s.stop
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


