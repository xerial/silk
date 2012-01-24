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
package xerial.silk
package util

import java.util.NoSuchElementException
import collection.mutable.{ArrayBuffer, WeakHashMap}

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
    val l = new Logger(rootLoggerName, new ConsoleLogOutput(), None)
    def getDefaultLogLevel: LogLevel.Value = {
      val default = LogLevel.INFO
      val p = System.getProperty("loglevel")
      if (p == null)
        default
      else
        try {
          LogLevel.withName(p.toUpperCase)
        }
        catch {
          case _: NoSuchElementException => {
            Console.err.println("Unknown log level: %s. Use %s log level instead." format(p, default))
            default
          }
        }
    }
    l.logLevel = Some(getDefaultLogLevel)
    l
  }

  /**
   * Hold logger instances in weakly referenced hash map to allow releasing instances when necessary
   */
  protected val loggerHolder = Cache[String, Logger](createLogger)

  def apply(cl: Class[_]): Logger = getLogger(cl)


  def getLogger(cl: Class[_]): Logger = {
    getLogger(cl.getName())
  }

  /**
   * Get the logger of the specified name. Logger names are
   * dot-separated list of package names. Logger naming should be the same with java package/class naming convention.
   */
  def getLogger(name: String): Logger = {
    if (name.isEmpty)
      rootLogger
    else
      loggerHolder(name)
  }

  private def createLogger(name: String): Logger = {
    if (LogConfig.enableColor)
      new Logger(name, new ConsoleLogOutput with ANSIColor)
    else
      new Logger(name, new ConsoleLogOutput)
  }

  private def parentName(name: String): String = {
    val p = name.split("""\.""")
    if (p.isEmpty)
      Logger.rootLoggerName
    else
      p.slice(0, p.length - 1).mkString(".")
  }

}


/**
 * Add logging support. Add this trait to your class to allow logging in the classs
 * @author leo
 */
trait Logging {

  import LogLevel._

  class FormattedLogMessage(format: String, args: ArrayBuffer[Any]) {
    def <<(arg: Any) = {
      args += arg;
      this
    }

    override def toString = {
      try {
        format.format(args.toArray: _*)
      }
      catch {
        case e:IllegalArgumentException => "invalid format:" + format
      }
    }
  }


  /**
   * Allows to write "hello %s" % "world", instead of "hello %s".format("world")
   */
  implicit def logMessage(format: String) = new {
    def %(arg: Any) = {
      val a = new ArrayBuffer[Any]
      a += arg
      new FormattedLogMessage(format, a)
    }
  }




  type LogFunction = (=> Any) => Boolean

  private val _loggerName: String = this.getClass.getName()
  def loggerName = _loggerName

  private[this] lazy val _self: Logger = Logger.getLogger(_loggerName)

  def fatal(message: => Any): Boolean = _self.fatal(message)

  def error(message: => Any): Boolean = _self.error(message)

  def warn(message: => Any): Boolean = _self.warn(message)

  def info(message: => Any): Boolean = _self.info(message)

  def debug(message: => Any): Boolean = _self.debug(message)

  def trace(message: => Any): Boolean = _self.trace(message)

  def log(logLevel: LogLevel)(message: => Any): Boolean = {
    _self.log(logLevel)(message)
  }

  def logger: Logger = _self
}


/**
 * Logger definition
 */
class Logger(val name: String, var out: LogOutput, parent: Option[Logger]) {
  protected var logLevel: Option[LogLevel] = None

  def this(name: String, out: LogOutput) = {
    this (name, out, Some(Logger.getLogger(Logger.parentName(name))))
  }


  def shortName: String = {
    name.split("""\.""").last
  }

  def fatal(message: => Any): Boolean = log(FATAL)(message)

  def error(message: => Any): Boolean = log(ERROR)(message)

  def warn(message: => Any): Boolean = log(WARN)(message)

  def info(message: => Any): Boolean = log(INFO)(message)

  def debug(message: => Any): Boolean = log(DEBUG)(message)

  def trace(message: => Any): Boolean = log(TRACE)(message)

  def log(l: LogLevel)(message: => Any): Boolean = {
    if (isEnabled(l)) {
      out.output(this, l, out.formatLog(this, l, message))
      true
    }
    else
      false
  }

  def isEnabled(level: LogLevel): Boolean = {
    level <= getLogLevel
  }

  def getLogLevel: LogLevel = {
    logLevel match {
      case Some(x) => x
      case None => {
        // delegate to the parent
        val l = if (parent.isDefined) {
          parent.get.getLogLevel
        }
        else INFO
        logLevel = Some(l)
        l
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

trait LogOutput {

  import LogLevel._

  def formatLog(l: Logger, lv: LogLevel, message: => Any): String
  def output(l: Logger, lv: LogLevel, message: String): Unit
}

class NullLogOutput extends LogOutput {
  def formatLog(l: Logger, lv: LogLevel, message: => Any): String = { message.toString }
  def output(l: Logger, lv: LogLevel, message: String): Unit = {}
}


class ConsoleLogOutput extends LogOutput {

  override def formatLog(l: Logger, lv: LogLevel, message: => Any): String = {
    def isMultiLine(str: String) = str.contains("\n")
    val s = {
      val m = message.toString
      if (isMultiLine(m))
        "\n" + m
      else
        m
    }
    if (s.length > 0)
      "[%s] %s".format(l.shortName, s)
    else
      ""
  }

  override def output(l: Logger, lv: LogLevel, message: String) {
    if (message.length > 0) {
      Console.out.println(message)
    }
  }
}

trait ANSIColor extends ConsoleLogOutput {
  val colorPrefix = Map[LogLevel.Value, String](
    ALL -> "",
    TRACE -> Console.GREEN,
    DEBUG -> "",
    INFO -> Console.CYAN,
    WARN -> Console.YELLOW,
    ERROR -> Console.MAGENTA,
    FATAL -> Console.RED,
    OFF -> "")

  override def output(l: Logger, lv: LogLevel, message: String): Unit = {
    if (message.length > 0) {
      val prefix = colorPrefix(lv)
      super.output(l, lv, "%s%s%s".format(prefix, message, Console.RESET))
    }
  }
}
