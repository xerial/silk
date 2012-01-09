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
  "Time block" should "measure the elapsed time of the code block" in {
    val t = time {
      var count = 0
      for(i <- 0 to 1000000) {
        count += 1
      }
    }
    debug(t.reportElapsedTime)
  }

  "Nested time blocks" should "accumulate the elapsed time" in {
    time {
      time {

      }
    }

  }

}