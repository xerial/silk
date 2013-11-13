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
import xerial.silk.SilkUtil
import xerial.silk.example.ExampleMain
import xerial.silk.util.{Path, Log4jUtil}
import java.net.URL
import xerial.silk.framework.ClassBox
import java.util.jar.{JarFile}
import java.io.File
import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}


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

  Log4jUtil.configureLog4j


  def modules = Seq(
    ModuleDef("cluster", classOf[ClusterCommand], "cluster management commands"),
    ModuleDef("example", classOf[ExampleMain], "example programs")
  )


  logLevel.foreach { l => LoggerFactory.setDefaultLogLevel(l) }

  @command(description = "Print env")
  def info = println("prog.home=" + System.getProperty("prog.home"))

  @command(description = "Show version")
  def version(@option(prefix="--buildtime", description="show build time")
              showBuildTime:Boolean=false)  {
    val s = new StringBuilder
    s.append(SilkUtil.getVersion)
    if(showBuildTime) {
      SilkUtil.getBuildTime foreach { buildTime =>
        s.append(s" ${DateFormat.getDateTimeInstance.format(new Date(buildTime))}")
      }
    }
    println(s.toString)
  }


  @command(description = "eval silk expression in a class")
  def eval(@option(prefix="-n,--dryrun", description="Dry run. Only shows what operation to be evaluated")
           isDryRun : Boolean = false,
           @argument
           target:String,
           @argument
           args:Array[String]) {

    val (clName:String, funOpt) = target.split(":") match {
      case Array(clName, fun) => (clName, Some(fun))
      case other => (other, None)
    }

    val cname = {
      val pos = clName.lastIndexOf(".")
      if(pos == -1)
        clName
      else
        clName.substring(pos+1)
    }

    info(s"target path:$clName, leaf:$cname, fun:$funOpt, args:[${args.mkString(",")}]")

    import scala.collection.JavaConversions._

    val classPathEntries = ClassBox.classPathEntries

    val classLoader = Thread.currentThread.getContextClassLoader

    val isFullPath = clName.lastIndexOf(".") != -1
    val clPath = s"${clName.replaceAll("\\.", "/")}.class"
    val clFile = s"${cname}.class"

    def removeExt(s:String) = s.replaceAll("\\.class$", "")

    def findTargetClassFile(resource:URL) : Option[String] = {
      if(ClassBox.isJarFile(resource)) {
        // Find the target class from a jar file
        val path = resource.getPath
        val pos: Int = path.indexOf("!")
        if(pos == -1)
          None
        else {
          val jarPath = path.substring(0, pos).replaceAll("%20", " ")
          val jarFilePath = jarPath.replace("file:", "")
          val jar = new JarFile(jarFilePath)

          val entryName = if(isFullPath)
            Option(jar.getEntry(s"/$clPath")).map(_.getName)
          else
            jar.entries.collectFirst{
              case e if e.getName.endsWith(clFile) =>
                e.getName
            }
          entryName.map(name => removeExt(name))
        }
      }
      else if(resource.getProtocol == "file") {
        // Find the target class from a directory
        @tailrec
        def find(lst:List[File]) : Option[File] = {
          if(lst.isEmpty)
            None
          else {
            val h = lst.head
            if(h.isDirectory)
              find(h.listFiles.toList ::: lst.tail)
            else {
              val fileName = h.getName
              if(fileName.endsWith(".class") && fileName == clFile)
                Some(h)
              else
                find(lst.tail)
            }
          }
        }

        import Path._
        val filePath = resource.getPath
        val base = new File(filePath)
        if(isFullPath) {
          // Search the target file by directly specifying the file name
          val f = new File(filePath, clPath)
          if(f.exists())
            Some(f.relativeTo(base).getPath)
          else
            None
        }
        else {
          // Search directories recursively
          find(List(base)).map{ f => f.relativeTo(base).getPath }
        }
      }
      else
        None
    }


    val targetClassName = classPathEntries.toIterator.map(findTargetClassFile).collectFirst{
      case Some(relativePathToClass) => {
        removeExt(relativePathToClass).replaceAll("\\/", ".")
      }
    }

    if(targetClassName.isEmpty) {
      error(s"class $clName is not found")
      return
    }


    Try(Class.forName(targetClassName.get, false, classLoader)) match {
      case Success(cl) =>
        info(s"target class: $cl")
      case Failure(e) =>
        error(e)
    }

    //Resource.findResourceURLs(Thread.currentThread.getContextClassLoader, "xerial").foreach(f => debug(s"$f"))
//      .filter(f => !f.isDirectory) // && f.logicalPath.endsWith(".class"))
//      .map{f =>
//         if(f.logicalPath.contains(cl))
//           info(s"Find ${f}")
//         else
//           debug(s"$f")
//    }


  }


}





