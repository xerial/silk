//--------------------------------------
//
// Log.scala
// Since: 2013/07/23 12:37 PM
//
//--------------------------------------

package xerial.silk.webui.app

import xerial.silk.webui.WebAction
import xerial.core.io.IOUtil
import java.io.FileInputStream
import scala.io.Source
import scala.tools.reflect

/**
 * @author Taro L. Saito
 */
class Log extends WebAction {

  import xerial.silk.cluster._
  import xerial.core.io.Path._
  val logDir = config.silkLogDir
  val logFile = logDir / s"${localhost.prefix}.log"

  val colorMap = Map(
    Console.BLACK -> "black",
    Console.RED -> "red",
    Console.GREEN -> "green",
    Console.YELLOW -> "yellow",
    Console.BLUE -> "blue",
    Console.MAGENTA -> "magenta",
    Console.CYAN -> "cyan",
    Console.WHITE -> "white")

  val colorESC = colorMap.keySet

  private def node = localhost.prefix

  private def logLines = {
    for(line <- Source.fromFile(logFile).getLines) yield {
      colorESC.find(line.startsWith(_)) map { colorPrefix =>
        val l = line.substring(colorPrefix.length).replaceAllLiterally(Console.RESET, "")
        s"""<font color="${colorMap(colorPrefix)}">$l</font>"""
      } getOrElse (line)
    }
  }

  def show {
    renderTemplate("log.ssp", Map("log" -> logLines.toSeq, "node" -> node))
  }

  def tail(rows:Int=50) {
    val tail = logLines.toSeq.takeRight(rows)
    renderTemplate("log.ssp", Map("log" -> tail, "node" -> node))
  }


}