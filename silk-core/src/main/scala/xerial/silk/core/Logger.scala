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
package log

import java.io.{PrintStream, Writer}
import collection.mutable.WeakHashMap

//--------------------------------------
//
// Logger.scala
// Since: 2012/01/07 9:19
//
//--------------------------------------

object LogLevel extends Enumeration {
  type LogLevel = Value
  val OFF, FATAL, ERROR, WARN, INFO, DEBUG, TRACE, ALL = Value
}

object LogConfig {
  val enableColor: Boolean = {
    val term = System.getenv("TERM")
    term != null || System.getProperty("log.color", "false") == "true"
  }

}

// import log level enums, FATAL, ERROR, DEBUG, ...

import LogLevel._

object Logger {

  val rootLoggerName = "root"
  val rootLogger = {
    val l = getLogger(rootLoggerName)
    val defaultLogLevel = LogLevel.withName(System.getProperty("loglevel", "INFO"))
    l.logLevel = Some(defaultLogLevel)
    l
  }

  /**
   * Hold logger instances in weakly referenced hash map to allow releasing instances when necessary
   */
  protected val loggerHolder = new WeakHashMap[String, Logger]()

  /**
   * Get the logger of the specified name. Logger names are
   * dot-separated list of package names. Logger naming should be the same with java package/class naming convention.
   */
  def getLogger(name: String): Logger = {
    loggerHolder.get(name) match {
      case Some(x) => x
      case None => {
        val newLogger = createLogger(name)
        loggerHolder += name -> newLogger
        newLogger
      }
    }
  }

  private def createLogger(name: String): Logger = {
    if (LogConfig.enableColor)
      new ConsoleLogger(name) with ANSIColor
    else
      new ConsoleLogger(name)
  }
}


/**
 * Add logging support. Add this trait to your class to allow logging in the classs
 * @author leo
 */
trait Logging {

  import LogLevel._

  type LogFunction = (=> AnyRef) => Boolean

  val name: String = this.getClass.getName()
  private[this] lazy val _self: Logger = Logger.getLogger(name)

  def fatal(message: => Any): Boolean = _self.log(TRACE, message)
  def error(message: => Any): Boolean = _self.log(ERROR, message)
  def warn(message: => Any): Boolean = _self.log(WARN, message)
  def info(message: => Any): Boolean = _self.log(INFO, message)
  def debug(message: => Any): Boolean = _self.log(DEBUG, message)
  def trace(message: => Any): Boolean = _self.log(TRACE, message)

  def log(logLevel: LogLevel)(message: => Any): Boolean = {
    _self.log(logLevel, message)
  }
}

trait LogOutput {
  def formatLog(level: LogLevel, message: => Any): Any = message
  def output(level: LogLevel, message: Any): Unit
}

/**
 * Logger definition
 */
abstract class Logger(val name: String) extends LogOutput {

  protected val parent: Option[Logger] = Some(Logger.getLogger(parentName))
  protected var logLevel: Option[LogLevel] = None

  def shortName: String = {
    name.split("""\.""").last
  }
  def parentName: String = {
    val p = name.split("""\.""")
    if (p.isEmpty)
      Logger.rootLoggerName
    else
      p.slice(0, p.length - 1).mkString(".")
  }

  def log(l: LogLevel, message: => Any): Boolean = {
    if (isEnabled(l)) {
      output(l, formatLog(l, message))
      true
    }
    else
      false
  }

  def isEnabled(level: LogLevel): Boolean = {
    getLogLevel <= level
  }

  def getLogLevel: LogLevel = {
    logLevel match {
      case Some(x) => x
      case None => {
        // delegate to the parent
        if (parent.isDefined) {
          parent.get.getLogLevel
        }
        else INFO
      }
    }
  }

  /**
   * Set the log level of this logger. 
   */
  def setLogLevel(l: LogLevel) = {
    def isDescendantOrSelf(loggerName: String) = {
      loggerName.startsWith(name)
    }
    // Reset the log level of all descendants of this logger
    for (desc <- Logger.loggerHolder.filterKeys(isDescendantOrSelf).values) {
      desc.logLevel = None
    }
    logLevel = Some(l)
  }

}


trait ANSIColor extends LogOutput {
  val colorPrefix = Map(
    ALL -> "",
    TRACE -> Console.GREEN,
    DEBUG -> "",
    INFO -> Console.CYAN,
    WARN -> Console.YELLOW,
    ERROR -> Console.MAGENTA,
    FATAL -> Console.RED,
    OFF -> "")

  def formatLog(level: LogLevel, message: String): String = {
    "%s%s%s".format(colorPrefix(level), message, Console.RESET)
  }

}

class ConsoleLogger(name: String) extends Logger(name) {

  override def formatLog(level: LogLevel, message: => Any): Any = {
    val s = message.toString
    if (s.contains("""[\n]"""))
      "\n" + s
    else
      s
  }

  override def output(level: LogLevel, message: Any) {
    Console.withErr(Console.err) {
      println("[%s] %s".format(level.toString, message))
    }
  }
}

