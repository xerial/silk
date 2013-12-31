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

package xerial.silk.cui

import xerial.core.log.{Logger, LoggerFactory, LogLevel}
import xerial.lens.cui._
import java.util.Date
import java.lang.reflect.InvocationTargetException
import java.text.DateFormat
import xerial.silk._
import xerial.silk.util.Log4jUtil
import scala.util.{Failure, Success, Try}
import xerial.lens.{MethodCallBuilder, Parameter, ObjectMethod, ObjectSchema}
import xerial.core.util.{StopWatch, Shell}
import xerial.silk.framework.Host
import xerial.silk.weaver.example.ExampleMain
import xerial.silk.framework.scheduler.ScheduleGraph
import xerial.lens.cui.ModuleDef
import scala.util.Failure
import scala.{Array, Some}
import scala.util.Success
import xerial.silk.cluster.SilkCluster
import xerial.silk.cui.{ClusterCommand, ClassFinder}
import xerial.lens.cui.ModuleDef
import scala.util.Failure
import scala.Some
import scala.util.Success


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


  /**
   * Check wheather silk is installed
   * @param h
   */
  def isSilkInstalled(h:Host) : Boolean = {
    val ret = Shell.exec("ssh -n %s '$SHELL -l -c silk version'".format(h.name))
    ret == 0
  }


}

trait DefaultMessage extends DefaultCommand {
  def default {
    println("silk %s".format(SilkUtil.getVersion))
    println(SilkMain.DEFAULT_MESSAGE)
  }

}


object FrameworkType {
  case object CLUSTER extends FrameworkType
  case object MEMORY extends FrameworkType

  val frameworkTypes = Seq(CLUSTER, MEMORY)
  val typeNameTable = (frameworkTypes.map { t => t.toString.toLowerCase -> t }).toMap[String, FrameworkType]

  def unapply(s:String) : Option[FrameworkType] = typeNameTable.get(s.toLowerCase)
}

abstract class FrameworkType

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

  import FrameworkType._

  @command(description = "eval silk expression in a class")
  def eval(@option(prefix="-d,--dryrun", description="Dry run. Only shows operations to be evaluated")
           isDryRun : Boolean = false,
           @option(prefix="-s,--show", description="Print result (default:false)")
           showResult : Boolean = false,
           @option(prefix="-f,--framework", description="framework. memory(default) or cluster")
           frameworkType : FrameworkType = MEMORY,
           @argument
           target:String,
           @argument
           args:Array[String]) {


    val (clName:String, funOpt) = target.split(":") match {
      case Array(clName, fun) => (clName, Some(fun))
      case other => (other, None)
    }



    val classLoader = Thread.currentThread().getContextClassLoader()
    val targetClassName = ClassFinder.findClass(clName, classLoader)
    if(targetClassName.isEmpty) {
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

    val targetMethodOrVal = funOpt.flatMap{f =>
      sc.methods.find(_.name == f) orElse sc.findParameter(f)
    }

    if(targetMethodOrVal.isEmpty) {
      error(s"method or val ${funOpt.get} is not found")
      return
    }

    info(s"Found ${targetMethodOrVal.get}")

    info(s"constructor: ${sc.findConstructor.getOrElse("None")}")
    sc.findConstructor.map { ct =>
      // Inject SilkEnv
      val env : Weaver = frameworkType match {
        case MEMORY =>
          info(s"Use in-memory framework")
          Weaver.inMemoryWeaver
        case CLUSTER =>
          info(s"Use cluster framework")
          SilkCluster.init
      }
      val owner = ct.newInstance(Array(env)).asInstanceOf[AnyRef]

      targetMethodOrVal.get match {
        case mt:ObjectMethod =>
          // Parse options
          val opt = new OptionParser(mt)
          val parseResult = opt.parse(args)
          // Feed parameters
          val b = parseResult.build(new MethodCallBuilder(mt, owner))
          val silk = b.execute
          silk match {
            case s:Silk[_] =>
              val g = ScheduleGraph(s)
              info(g)
              if(!isDryRun) {
                val timer = new StopWatch
                val resultFuture = s match {
                  case s:SilkSingle[_] => env.weave(s)
                  case s:SilkSeq[_] => env.weave(s)
                }
                info(s"Evaluation of ${mt.name} finished in ${timer.reportElapsedTime}")
                if(showResult) {
                  resultFuture.get match {
                    case s:Seq[_] => println(s"${s.mkString(", ")}")
                    case other => println(other)
                  }
                }
              }
            case other => println(other)
          }
        case vl:Parameter =>
      }

      frameworkType match {
        case CLUSTER =>
          SilkCluster.cleanUp
        case _ =>
      }

    }


  }


}





