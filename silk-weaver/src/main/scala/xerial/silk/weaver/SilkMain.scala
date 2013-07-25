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

package xerial.silk.weaver

import xerial.core.log.{Logger, LoggerFactory, LogLevel}
import xerial.lens.cui._
import java.util.Date
import java.lang.reflect.InvocationTargetException
import java.text.DateFormat
import xerial.silk.{SilkUtil, cluster}
import xerial.silk.example.ExampleMain


//--------------------------------------
//
// CUIMain.scala
// Since: 2012/01/24 11:44
//
//--------------------------------------

/**
 * Program entry point of silk
 *
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
        error(e.getTargetException)
      case e:Exception =>
        error(e)
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




}

trait DefaultMessage extends DefaultCommand {
  def default {
    println("silk %s".format(SilkUtil.getVersion))
    println(SilkMain.DEFAULT_MESSAGE)
  }

}

/**
 * Command-line interface of silk
 * @param help
 * @param logLevel
 */
class SilkMain(@option(prefix="-h,--help", description="display help message", isHelp = true)
               help:Boolean=false,
               @option(prefix="-l,--loglevel", description="set loglevel. trace|debug|info|warn|error|fatal|off")
               logLevel:Option[LogLevel] = None
                )  extends DefaultMessage with CommandModule with Logger {

  cluster.configureLog4j


  def modules = Seq(
    ModuleDef("cluster", classOf[ClusterCommand], "cluster management commands"),
    ModuleDef("example", classOf[ExampleMain], "example programs")
  )

  logLevel.foreach { l => LoggerFactory.setDefaultLogLevel(l) }

  @command(description = "Print env")
  def info = println("prog.home=" + System.getProperty("prog.home"))

  @command(description = "Show version")
  def version(@option(prefix="--buildtime", description="show build time") showBuildTime:Boolean=false)  {
    val s = new StringBuilder
    s append "%s".format(SilkUtil.getVersion)
    if(showBuildTime) {
      SilkUtil.getBuildTime foreach { buildTime =>
        s append " %s".format(DateFormat.getDateTimeInstance.format(new Date(buildTime)))
      }
    }
    println(s.toString)
  }




}





