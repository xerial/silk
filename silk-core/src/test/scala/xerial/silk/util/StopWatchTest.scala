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


import xerial.silk.core.SilkSpec
import actors.Actor

//--------------------------------------
//
// StopWatchTest.scala
// Since: 2012/01/09 8:44
//
//--------------------------------------

/**
 * @author leo
 */
class StopWatchTest extends SilkSpec {

  import StopWatch._

  "Time measure" should "be able to create time block" in {
    val t = time {}
    debug(t.report)
    val t2 = time("block1") { }
    debug(t2.report)
  }

  "Time measure" should "report the elapsed time" in {
    val c = new TimeMeasure {
      val name: String = "root"
      def body() = {
        "hello world"
      }
    }
    debug(c)


  }


  "Time block" should "measure the elapsed time of the code block" in {
    val t: TimeMeasure = time("main") {
      var count = 0
      for (i <- 0 to 1000000) {
        count += 1
      }
    }
    debug(t)

  }

  "Nested time blocks" should "accumulate the elapsed time" in {
    val t = time("main") {
      time {

      }
    }

    debug(t)
  }

}