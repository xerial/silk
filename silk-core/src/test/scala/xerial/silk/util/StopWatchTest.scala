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

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

//--------------------------------------
//
// StopWatchTest.scala
// Since: 2012/01/09 8:44
//
//--------------------------------------

/**
 * @author leo
 */
@RunWith(classOf[JUnitRunner])
class StopWatchTest extends SilkSpec {

  import PerformanceLogger._

  "Time measure" should "be able to create time block" in {
    time("main") {}
    time("block1") { }
  }


  "Time block" should "measure the elapsed time of the code block" in {
    val t: TimeMeasure = time("main") {
      var count = 0
      for (i <- 0 to 100000) {
        count += 1
      }
    }

  }

  "Nested time blocks" should "accumulate the elapsed time" in {
    val t = time("main") {
      for (i <- 0 until 100) {
        block("A") {
          var v = 0
          for(i<-0 until 100000)
            v += i
        }
      }
    }

    t.executionCount must be (1)
    val a_block = t("A")
    a_block.executionCount must be (100)
  }

  "Time blocks in different context" should "be independent" in {
    def sub = {
      time("sub") {
      }
    }

    time("main") {
      sub
    }
  }

  "Time blocks" should "support repetitive execution" in {
    val t = time("code", repeat=10) {
      var v = 2
      block("loop", repeat=2) {
        for(i<-0 until 100000)
          v += i
      }
    }

    t.executionCount must be (10)
  }

}