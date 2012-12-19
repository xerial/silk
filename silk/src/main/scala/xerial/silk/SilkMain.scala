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

import cluster.{ClusterCommand, SilkClientCommand, SilkClient}
import cluster.SilkClient.Terminate
import java.io.{FileReader, BufferedReader, File}
import scala.io.Source
import xerial.core.log.{Logger, LoggerFactory, LogLevel}
import xerial.lens.cui._
import java.util.{Date, Properties}
import java.lang.reflect.InvocationTargetException
import javax.print.attribute.standard.DateTimeAtCompleted
import java.text.DateFormat


//--------------------------------------
//
// CUIMain.scala
// Since: 2012/01/24 11:44
//
//--------------------------------------

/**
 * @author leo 
 */
object SilkMain extends Logger {


  private def wrap[U](f: => U) : Int = {
    try {
      f
      0
    }
    catch {
      case e:InvocationTargetException =>
        error(e.getMessage)
        error(e.getTargetException)
        e.getTargetException.printStackTrace
      case e:Exception =>
        error(e.getMessage)
        e.printStackTrace()
    }
    -1
  }

  def main(argLine:String) : Int = {
    wrap {
      Launcher.of[SilkMain].execute[SilkMain](argLine)
    }
  }

  def main(args: Array[String]) {
    wrap {
      Launcher.of[SilkMain].execute[SilkMain](args)
    }
  }

  val DEFAULT_MESSAGE = "Type --help for the list of sub commands"


  private def getVersionFile = {
    val home = System.getProperty("prog.home")
    new File(home, "VERSION")
  }

  def getVersion : String = {
    val versionFile = getVersionFile
    val versionNumber =
      if (versionFile.exists()) {
        // read properties file
        val prop = (for{
          line <- Source.fromFile(versionFile).getLines;
          val c = line.split(":=");
          pair <- if(c.length == 2) Some((c(0).trim, c(1).trim)) else None
        } yield pair) toMap

        prop.get("version")
      }
      else
        None

    val v = versionNumber getOrElse "unknown"
    v
  }

  def getBuildTime : Option[Long] = {
    val versionFile = getVersionFile
    if (versionFile.exists())
      Some(versionFile.lastModified())
    else
      None
  }


}

trait DefaultMessage extends DefaultCommand {
  def default {
    println("silk %s".format(SilkMain.getVersion))
    println(SilkMain.DEFAULT_MESSAGE)
  }

}


class SilkMain(@option(prefix="-h,--help", description="display help message", isHelp = true)
               help:Boolean=false,
               @option(prefix="-l,--loglevel", description="set loglevel. trace|debug|info|warn|error|fatal|off")
               logLevel:Option[LogLevel] = None
                )  extends DefaultMessage with CommandModule with Logger {

  configureLog4j


  def modules = Seq(
    ModuleDef("client", classOf[SilkClientCommand], "client management commands"),
    ModuleDef("cluster", classOf[ClusterCommand], "cluster management commands")
  )

  logLevel.foreach { l => LoggerFactory.setDefaultLogLevel(l) }

  @command(description = "Print env")
  def info = println("prog.home=" + System.getProperty("prog.home"))

  @command(description = "Show version")
  def version(@option(prefix="--buildtime", description="show build time") showBuildTime:Boolean=false)  {
    val s = new StringBuilder
    s append "%s".format(SilkMain.getVersion)
    if(showBuildTime) {
      SilkMain.getBuildTime foreach { buildTime =>
        s append " %s".format(DateFormat.getDateTimeInstance.format(new Date(buildTime)))
      }
    }
    println(s.toString)
  }




}





