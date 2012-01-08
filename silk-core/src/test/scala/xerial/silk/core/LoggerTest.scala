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

package xerial.silk.core

import org.scalatest.FlatSpec
import org.scalatest.matchers.{MustMatchers, ShouldMatchers}

//--------------------------------------
//
// LoggerTest.scala
// Since: 2012/01/07 15:38
//
//--------------------------------------

/**
 * @author leo
 */
class LoggerTest extends FlatSpec with ShouldMatchers with MustMatchers {

  "root logger" must "be present" in {
    val l = Logger.rootLogger
    l.log(LogLevel.INFO) { "root logger" }
  }
  
  class A extends Logging {

    def testSuite(f: LogFunction) {
      f.apply("Hello")
      f.apply("%s world! %d".format("Hello", 2012))
    }

    debug {
      "debug message"
    }
    trace {
      "trace message"
    }
    log(LogLevel.DEBUG) {
      "debug log message"
    }

    val loggerType: List[LogFunction] = List(fatal, error, warn, info, debug, trace)
    loggerType.foreach {
      testSuite(_)
    }
  }
  
  "class that extends logger" should  "display log according to log level" in {
    val a = new A
  }

}