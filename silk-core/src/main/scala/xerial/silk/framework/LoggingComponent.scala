package xerial.silk.framework

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
    def formatLog(s: String): String
    def trace(s: => String)
    def debug(s: => String)
    def info(s: => String)
    def warn(s: => String)
    def error(s: => String)
    def fatal(s: => String)
  }

  /**
   * Log
   * @param s
   */
  def trace(s: => String) { logger.trace(logger.formatLog(s)) }
  def debug(s: => String) { logger.debug(logger.formatLog(s)) }
  def info(s: => String) { logger.info(logger.formatLog(s)) }
  def warn(s: => String) { logger.warn(logger.formatLog(s)) }
  def error(s: => String) { logger.error(logger.formatLog(s)) }
  def fatal(s: => String) { logger.fatal(logger.formatLog(s)) }

}



/**
 * Default logging implementation
 */
trait DefaultConsoleLogger extends LoggingComponent {

  type Logger = LoggerImpl
  val logger = new LoggerImpl

  class LoggerImpl extends LoggerAPI {
    val logger = LoggerFactory("xerial.silk")
    def formatLog(s: String) = s
    def trace(s: => String) { logger.trace(s) }
    def debug(s: => String) { logger.debug(s) }
    def info(s: => String) { logger.info(s) }
    def warn(s: => String) { logger.warn(s) }
    def error(s: => String) { logger.error(s) }
    def fatal(s: => String) { logger.fatal(s) }
  }

}

/**
 * Logger with host address
 */
trait HostLogger extends DefaultConsoleLogger {

  override val logger = new HostLoggerImpl

  class HostLoggerImpl extends LoggerImpl {
    private val currentHost = InetAddress.getLocalHost.getHostAddress
    override def formatLog(log: String) = s"[$currentHost] $log"
  }

}

