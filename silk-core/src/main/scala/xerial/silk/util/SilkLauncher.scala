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

//--------------------------------------
//
// SilkLauncher.scala
// Since: 2012/03/22 14:28
//
//--------------------------------------

object SilkLauncher {

  def of[A <: SilkCommandModule](implicit m:ClassManifest[A]) = {
    new SilkLauncher[A]
  }

}

trait SilkCommand {
  def displayHelp

}


trait SilkCommandModule extends Logger { self =>
  type A = self.type

  val moduleName : String

  @option(symbol="h", description="display help message")
  protected var displayHelp : Boolean = false

  @argument(index = 0, name = "command name", description="sub command name")
  protected var commandName : Option[String] = None

  //private val moduleList = new ArrayBuffer[SilkCommandModule]

  //def modules = moduleList.toArray

//  def add(module:SilkCommandModule) = {
//    moduleList += module
//  }

  def execute(unusedArgs:Array[String]) : Any = {

    debug ("display help:%s, unused args:%s", displayHelp, unusedArgs.mkString(", "))
    if(displayHelp) {
      info("command name:%s", commandName)
      commandName match {
        case Some(cmd) => printUsage(cmd)
        case None => printUsage
      }
    }
    else {
      commandName match {
        case Some(cmd) => {
          info("execute command:" + cmd)
        }
        case None => printProgName
      }
    }


  }

  def printProgName {

  }

  def printUsage {
    info("print usage")
    OptionParser(this.getClass).printUsage
    
    val schema = ObjectSchema(this.getClass)
    
    println("[commands]")
    schema.methods.foreach{m =>
      m.findAnnotationOf[xerial.silk.util.command].map{ cmd =>
        println(" %-10s\t%s".format(m.name, cmd.description))
      }
    }
    
  }

  def printUsage(commandName:String) {
    val schema = ObjectSchema(this.getClass)
    //schema.methods.flatMap(m => m.findAnnotationOf[])
  }

}

/**
 * CommandTrait-launcher
 *
 * @author leo
 */
class SilkLauncher[A <: SilkCommandModule](implicit m:ClassManifest[A]) extends Logger {
  private val cl : Class[_] = m.erasure

  def execute(argLine:String) : Any =
    execute(CommandLineTokenizer.tokenize(argLine))


  def execute(args:Array[String]) : Any = {
    val parser = OptionParser.of[A]
    val (module, parseResult) = parser.build[A](args)

    module.execute(parseResult.unusedArgument)
  }

  def execute(input:DataProducer) : Any = {



  }


}
