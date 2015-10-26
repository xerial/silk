/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package xerial.silk.cui

import java.lang.reflect.InvocationTargetException
import java.text.DateFormat
import java.util.Date

import xerial.core.log.{LogLevel, Logger, LoggerFactory}
import xerial.lens.cui._
import xerial.lens.{MethodCallBuilder, ObjectMethod, ObjectSchema, Parameter}
import xerial.silk.core.util.SilkUtil
import xerial.silk.core.Silk
import xerial.silk.weaver.Weaver

import scala.tools.nsc.interpreter.ILoop
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
  private def wrap[U](f: => U): Int = {
    try {
      f
      0
    }
    catch {
      case e: InvocationTargetException =>
        error(e.getTargetException)
      case e: Exception =>
        error(e)
    }
    -1
  }

  def main(argLine: String): Int = {
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
class SilkMain(@option(prefix = "-h,--help", description = "display help message", isHelp = true)
               help: Boolean = false,
               @option(prefix = "-l,--loglevel", description = "set loglevel. trace|debug|info|warn|error|fatal|off")
               logLevel: Option[LogLevel] = None
                ) extends DefaultMessage with CommandModule with Logger {


  def modules = Seq(
  )


  logLevel.foreach { l => LoggerFactory.setDefaultLogLevel(l) }

  @command(description = "Print env")
  def info = println("prog.home=" + System.getProperty("prog.home"))

  @command(description = "Show version")
  def version(@option(prefix = "--buildtime", description = "show build time")
              showBuildTime: Boolean = false) {
    val s = new StringBuilder
    s.append(SilkUtil.getVersion)
    if (showBuildTime) {
      SilkUtil.getBuildTime foreach { buildTime =>
        s.append(s" ${DateFormat.getDateTimeInstance.format(new Date(buildTime))}")
      }
    }
    println(s.toString)
  }

  @command(description = "Launch development server")
  def server: Unit = {




  }


  @command(description = "eval silk expression in a class")
  def eval(@option(prefix = "-d,--dryrun", description = "Dry run. Only shows operations to be evaluated")
           isDryRun: Boolean = false,
           @option(prefix = "-s,--show", description = "Print result (default:false)")
           showResult: Boolean = false,
           @argument
           target: String,
           @argument
           args: Array[String]) {


    val (clName: String, funOpt) = target.split(":") match {
      case Array(clName, fun) => (clName, Some(fun))
      case other => (other, None)
    }


    val classLoader = Thread.currentThread().getContextClassLoader()
    val targetClassName = ClassFinder.findClass(clName, classLoader)
    if (targetClassName.isEmpty) {
      error(s"class $clName is not found")
      return
    }

    val targetClass = Try(Class.forName(targetClassName.get, true, classLoader)) match {
      case Success(cl) =>
        info(s"Target class $cl")
        cl
      case Failure(e) =>
        error(e)
        return
    }

    // Find a method or variable corresponding to the target
    val sc = ObjectSchema(targetClass)

    val targetMethodOrVal = funOpt.flatMap { f =>
      sc.methods.find(_.name == f) orElse sc.findParameter(f)
    }

    if (targetMethodOrVal.isEmpty) {
      error(s"method or val ${funOpt.get} is not found")
      return
    }

    info(s"Found ${targetMethodOrVal.get}")

    info(s"constructor: ${sc.findConstructor.getOrElse("None")}")
    sc.findConstructor.map { ct =>
      // Inject Weaver
      val weaver: Weaver = {
        info(s"Use in-memory framework")
        // TODO
        null
      }
      val owner = ct.newInstance(Array(weaver)).asInstanceOf[AnyRef]

      targetMethodOrVal.get match {
        case mt: ObjectMethod =>
          // Parse opti
          val opt = new OptionParser(mt)
          val parseResult = opt.parse(args)
          // Feed parameters
          val b = parseResult.build(new MethodCallBuilder(mt, owner))
          val silk = b.execute
          silk match {
            case s: Silk[_] =>
            //              val g = ScheduleGraph(s)
            //              info(g)
            //              if (!isDryRun) {
            //                val timer = new StopWatch
            //                val resultFuture = s match {
            //                  case s: SilkSingle[_] => weaver.weave(s)
            //                  case s: SilkSeq[_] => weaver.weave(s)
            //                }
            //                info(s"Evaluation of ${mt.name} finished in ${timer.reportElapsedTime}")
            //                if (showResult) {
            //                  resultFuture.get match {
            //                    case s: Seq[_] => println(s"${s.mkString(", ")}")
            //                    case other => println(other)
            //                  }
            //                }
            //              }
            case other => println(other)
          }
        case vl: Parameter =>
      }
    }


  }


}





