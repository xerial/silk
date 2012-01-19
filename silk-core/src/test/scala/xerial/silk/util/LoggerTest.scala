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

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

//--------------------------------------
//
// LoggerTest.scala
// Since: 2012/01/07 15:38
//
//--------------------------------------

/**
 * @author leo
 */
@RunWith(classOf[JUnitRunner])
class LoggerTest extends SilkSpec {

  "root logger" must "be present" in {
    val l = Logger.rootLogger
    l.log(LogLevel.INFO) {
      "root logger"
    }
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

    info {
      "%s log" format "formatted"
    }

    info("%s log" % "formatted in different syntax")

    info(
      "Hello %s, %d" % "world" << 2200
    )

    log(LogLevel.DEBUG) {
      "debug log message"
    }

    val loggerType: List[LogFunction] = List(fatal, error, warn, info, debug, trace)
    loggerType.foreach {
      testSuite(_)
    }
  }

  "class that extends logger" should "display log according to log level" in {
    val a = new A
  }

  "logger" should "support ANSI color" in {
    val prev = System.getProperty("log.color", "false")
    try {
      System.setProperty("log.color", "true")
      class Sample extends Logging {
        info {
          "info log"
        }
        debug {
          "debug log"
        }
      }

      new Sample
    }
    finally {
      System.setProperty("log.color", prev)
    }

  }

  "Disabled loggers" should "be faster than enabled ones" in {

    val l = Logger(this.getClass)
    val lv = l.getLogLevel
    val out = l.out
    try {
      l.setLogLevel(LogLevel.INFO)
      l.out = new NullLogOutput
      import PerformanceLogger._
      val t = time("log performance", repeat = 10) {
        val rep = 100

        block("debug log", repeat = rep) {
          debug {
            "%s %s!" % "hello" << "world"
          }
        }

        block("info log", repeat = rep) {
          info {
            "%s %s!" % "hello" << "world"
          }
        }
      }
      t("debug log").elapsedSeconds should be <= (t("info log").elapsedSeconds)
    }
    finally {
      l.setLogLevel(lv)
      l.out = out
    }


  }

}