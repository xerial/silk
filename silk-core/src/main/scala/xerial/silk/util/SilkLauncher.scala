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
import collection.mutable.ArrayBuffer

//--------------------------------------
//
// SilkLauncher.scala
// Since: 2012/03/22 14:28
//
//--------------------------------------

object SilkLauncher {
  def apply(obj:AnyRef) = new SilkLauncher(obj)

  def of[A](implicit m:ClassManifest[A]) = {
    val cl = m.erasure
    val obj = TypeUtil.newInstance(cl).asInstanceOf[AnyRef]
    apply(obj)
  }

}

trait SilkCommand {
  def displayHelp

}

trait HelpOption {
  self : SilkCommandModule =>

  @option(symbol="h", description="display help message")
  protected var displayHelp : Boolean = false

  @argument(index = 0, name = "command name", description="sub command name")
  protected var commandName : Option[String] = None

  def execute(argLine:String) : Unit = execute(CommandLineTokenizer.tokenize(argLine))

  def execute(args:Array[String]) : Unit = {
    if(displayHelp) {
      commandName match {
        case Some(cmd) => printUsage(cmd)
        case None => printUsage
      }
    }
    else {
      commandName match {
        case Some(cmd) => SilkLauncher(this).call(args)
        case None => printProgName
      }
    }
  }

  def printProgName {

  }

  def printUsage {
    
  }
  
  def printUsage(commandName:String) {
    val schema = ObjectSchema(this.getClass)
    //schema.methods.flatMap(m => m.findAnnotationOf[])
  }
}

trait SilkCommandModule {
  val moduleName : String
  
  private val moduleList = new ArrayBuffer[SilkCommandModule]

  def modules = moduleList.toArray

  def add(module:SilkCommandModule) = {
    moduleList += module
  }
  
  def execute : Unit = {}
  
}



/**
 * Command-launcher
 *
 * @author leo
 */
class SilkLauncher(obj:AnyRef) extends Logger {
  private val cl : Class[_] = obj.getClass
  private val schema = ObjectSchema(cl)


  def call(argLine:String) : Any = {
    val args = CommandLineTokenizer.tokenize(argLine)
    call(args)
  }

  def call(args:Array[String]) : Any = {
    if(args.length <= 0)
      sys.error("no command name specified")

    val commandName = args(0)
    schema.methods.find(m => m.name == commandName) match {
      case None => sys.error("unknown command: %s".format(commandName))
      case Some(cm) => {
        // TODO parse args
        cm.argTypes

        // Invoke method
        val ret = cm.jMethod.invoke(obj)
        ret
      }
    }
  }

  def call(input:DataProducer) : Any = {



  }


}
