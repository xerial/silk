package xerial.silk.util

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

  def time(f:  => Unit) : StopWatch = {
    val s = new StopWatch
    try{
       f
    }
    finally {
      s.stop
    }
    s
  }

}

class StopWatch {

  // initialize the timer
  reset

  private object State extends Enumeration {
    val RUNNING, STOPPED = Value
  }

  private var lastSystemTime : Double = System.nanoTime
  private var accumulatedElapsedTime : Double = 0L
  private var state = State.RUNNING

  /**
   * Gets the elapsed time since this instance is created in seconds.
   *
   * @return the elapsed time in seconds.
   */
  def getElapsedTime: Double = {
    val NANO_UNIT : Double = 1000000000L
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

  def resume() {
    if (state == State.RUNNING)
      return

    lastSystemTime = System.nanoTime();
    state = State.RUNNING;
  }

  def reportElapsedTime : String = {
    "%.2f sec." format (getElapsedTime)
  }


}


