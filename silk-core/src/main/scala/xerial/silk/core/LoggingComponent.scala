package xerial.silk.core

import java.net.InetAddress

import xerial.core.log.LoggerFactory

/**
 * A component for logging
 */
trait LoggingComponent {

  type Logger <: LoggerAPI
  val logger: Logger

  trait LoggerAPI {
    /**
     * Format log message
     * @param s
     * @return
     */
    def formatLog(s: Any): String
    def trace(s: => Any)
    def debug(s: => Any)
    def info(s: => Any)
    def warn(s: => Any)
    def error(s: => Any)
    def fatal(s: => Any)
  }

  /**
   * Log
   * @param s
   */
  def trace(s: => Any) { logger.trace(logger.formatLog(s)) }
  def debug(s: => Any) { logger.debug(logger.formatLog(s)) }
  def info(s: => Any) { logger.info(logger.formatLog(s)) }
  def warn(s: => Any) { logger.warn(logger.formatLog(s)) }
  def error(s: => Any) { logger.error(logger.formatLog(s)) }
  def fatal(s: => Any) { logger.fatal(logger.formatLog(s)) }

}



/**
 * Default logging implementation
 */
trait DefaultConsoleLogger extends LoggingComponent { self =>

  type Logger = LoggerImpl
  val logger = new LoggerImpl

  class LoggerImpl extends LoggerAPI {
    private val logger = LoggerFactory(self.getClass) // ("xerial.silk")
    def formatLog(s: Any) = s.toString
    def trace(s: => Any) { logger.trace(s) }
    def debug(s: => Any) { logger.debug(s) }
    def info(s: => Any) { logger.info(s) }
    def warn(s: => Any) { logger.warn(s) }
    def error(s: => Any) { logger.error(s) }
    def fatal(s: => Any) { logger.fatal(s) }
  }

}

/**
 * Logger with host address
 */
trait HostLogger extends DefaultConsoleLogger {

  override val logger = new HostLoggerImpl

  class HostLoggerImpl extends LoggerImpl {
    private val currentHost = InetAddress.getLocalHost.getHostAddress
    override def formatLog(log: Any) = s"[$currentHost] $log"
  }

}

