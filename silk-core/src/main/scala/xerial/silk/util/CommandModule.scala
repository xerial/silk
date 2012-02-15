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

//--------------------------------------
//
// CommandModule.scala
// Since: 2012/02/13 16:14
//
//--------------------------------------

/**
 * @author leo
 */
object CommandModule {

  trait Command {
    val name: String
    val oneLineDescription: String
    val optionHolder: AnyRef

    def execute(args: Array[String])
  }

  class DefaultGlobalOption {
    @option(symbol = "h", longName = "help", description = "Display help messages")
    var displayHelp = false
  }

  trait Module {
    protected val moduleOption = new DefaultGlobalOption()

    val optionHolder = moduleOption

    val name: Option[String] = None

    def execute(args: Array[String]) = {
      val opt = new OptionParser(optionHolder)
      val remaining = opt.parseUntilFirstArgument(args)

    }
  }


}