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

import cluster.SilkClient
import cluster.SilkClient.Terminate
import java.io.{FileReader, BufferedReader, File}
import scala.io.Source
import xerial.core.log.{Logger, LoggerFactory, LogLevel}
import xerial.lens.cui._
import java.util.Properties


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

  val DEFAULT_MESSAGE = "Type --help for the list of sub commands"

}

class SilkMain(@option(prefix="-h,--help", description="display help message", isHelp = true)
               help:Boolean=false,
               @option(prefix="-l,--loglevel", description="set loglevel. trace|debug|info|warn|error|fatal|off")
               logLevel:Option[LogLevel] = None
                )  extends DefaultCommand with Logger {

  // logLevel.foreach { l => LoggerFactory.setDefaultLogLevel(, l) }

  @command(description = "Print env")
  def info = println("prog.home=" + System.getProperty("prog.home"))

  @command(description = "Show version")
  def version = {
    val home = System.getProperty("prog.home")
    val versionFile = new File(home, "VERSION")

    val versionNumber =
      if (home != null && versionFile.exists()) {
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
    println("silk %s".format(v))
    v
  }

  @command(description = "Launch a Silk client in this machine")
  def client  {
    SilkClient.startClient
  }

  @command(description = "Terminate a silk client")
  def terminateClient(@argument hostname:String) = {
    val client = SilkClient.getClientAt(hostname)
    import akka.pattern.ask
    info("sending termination sygnal")
    client ! Terminate
  }


  def default {
    version
    println(SilkMain.DEFAULT_MESSAGE)
  }





}


