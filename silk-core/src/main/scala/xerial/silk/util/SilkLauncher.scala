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

package xerial.silk.util

import io.DataProducer
import xerial.silk.lens.ObjectSchema
import xerial.silk.lens.ObjectSchema.Method


//--------------------------------------
//
// SilkLauncher.scala
// Since: 2012/03/22 14:28
//
//--------------------------------------

object SilkLauncher {

  def of[A <: SilkCommandModule](implicit m: ClassManifest[A]) = {
    new SilkLauncher[A]
  }

}

trait SilkCommand {
  def displayHelp

}


trait SilkCommandModule extends Logger {
  self =>
  type A = self.type

  val moduleName: String

  @option(symbol = "h", description = "display help message")
  var displayHelp: Boolean = false

  @argument(index = 0, name = "command name", description = "sub command name")
  var commandName: Option[String] = None

  def execute(unusedArgs: Array[String]): Any = {

    trace("display help:%s, unused args:%s", displayHelp, unusedArgs.mkString(", "))
    if (displayHelp) {
      trace("command name:%s", commandName)
      commandName match {
        case Some(cmd) => printUsage(cmd)
        case None => printUsage
      }
    }
    else {
      commandName match {
        case Some(cmd) => {
          val cname = CName(cmd)
          trace("execute command:" + cname)
          val result = commandList.find(e => CName(e.name) == cname).map { c =>
            val parser = new OptionParser(c.method)
            val builder = new MethodCallBuilder(c.method, this)
            parser.build(unusedArgs, builder)
            builder.execute
          }
          if(result.isDefined) result.get else null
        }
        case None => printProgName
      }
    }

  }

  def printProgName {
    println("Type --help to see the list of commands")
  }

  private lazy val commandList : Seq[CommandDef] = ObjectSchema(this.getClass).methods.flatMap {
    m =>
      m.findAnnotationOf[command] match {
        case Some(cmd) => Some(new CommandDef(m, cmd))
        case _ => None
      }
  }

  def printUsage {
    trace("print usage")
    OptionParser(this.getClass).printUsage

    println("[commands]")
    commandList.foreach {
      c =>
        println(" %-10s\t%s".format(c.name, c.description))
    }

  }

  def printUsage(commandName: String) {
    trace("print usage of %s", commandName)
    commandList.find(_.name == commandName).map{ c =>
      val parser = new OptionParser(c.method)
      parser.printUsage
    }
  }




}

class CommandDef(val method: Method, val command: command) {
  val name = method.name
  val description = command.description
}


/**
 * CommandTrait-launcher
 *
 * @author leo
 */
class SilkLauncher[A <: SilkCommandModule](implicit m: ClassManifest[A]) extends Logger {
  private val cl: Class[_] = m.erasure

  def execute(argLine: String): Any =
    execute(CommandLineTokenizer.tokenize(argLine))

  def execute(args: Array[String]): Any = {
    val parser = OptionParser.of[A]
    val (module, parseResult) = parser.build[A](args)

    module.execute(parseResult.unusedArgument)
  }

  def execute(input: DataProducer): Any = {

  }

}
