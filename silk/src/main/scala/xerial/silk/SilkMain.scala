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

import java.io.File
import xerial.lens.cui.{option, command, Launcher, CommandModule}
import scala.io.Source
import xerial.core.log.{LoggerFactory, LogLevel}


//--------------------------------------
//
// CUIMain.scala
// Since: 2012/01/24 11:44
//
//--------------------------------------

/**
 * @author leo
 */
object SilkMain {

  def main(argLine:String) {
    Launcher.of[SilkMain].execute(argLine)
  }

  def main(args: Array[String]): Unit = {
    Launcher.of[SilkMain].execute(args)
  }

}

class SilkMain(@option(prefix="-h,--help", description="display help message", isHelp = true)
               help:Boolean=false,
               @option(prefix="-l,--loglevel", description="set loglevel. trace|debug|info|warn|error|fatal|off")
               logLevel:Option[LogLevel] = None
                )  {

  // logLevel.foreach { l => LoggerFactory.setDefaultLogLevel(, l) }

  @command(description = "Print env")
  def info = println("silk.home=" + System.getProperty("silk.home"))

  @command(description = "Show version")
  def version = {
    val home = System.getProperty("silk.home")
    val versionFile = new File(home, "VERSION")

    val versionNumber =
      if (home != null && versionFile.exists())
        Source.fromFile(versionFile).getLines().toArray.headOption
      else
        None

    println("silk: version %s".format(versionNumber.getOrElse("unknown")))
  }

  def printProgName = {
    version
    println("type --help for the list of sub commands")
  }
}


