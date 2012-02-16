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
// CommandModuleTest.scala
// Since: 2012/02/13 16:15
//
//--------------------------------------


object CommandModuleTest {

  class MyModule extends CommandLauncher {
    val oneLineDescription = "sample module"
  }

  class Hello extends Command {
    val commandName = "hello"
    val oneLineDescription = "say hello"

    def execute(args: Array[String]) = {
      println("Hello!")
    }
  }

  class Ping extends Command {
    val commandName = "ping"
    val oneLineDescription = "ping-pong"

    @option(symbol = "n", description = "# of repetitions")
    var times: Int = 3

    def execute(args: Array[String]) = {
      for(i <- 0 until times) {
        println("ping pong!")
      }
    }
  }

  class NestedModule extends CommandModule {
    val oneLineDescription = "nested module"
    val commandName = "nest"

  }


}

/**
 * @author leo
 */
class CommandModuleTest extends SilkSpec {

  import CommandModuleTest._



  "CommandModule" should {

    "accept help option (-h)" in {
      val m = new MyModule()
      m.execute("-h")
    }

    "print usage" in {
      val m = new MyModule()
      m.printUsage
    }


    "add commands" in {
      def m = new MyModule().addCommands(new Hello, new Ping)
      m.printUsage

      debug("empty command")
      m.execute("")

      debug("single command")
      m.execute("hello")

      debug("ping command")
      m.execute("ping -n 1")
    }

    "allow nested modules" in {
      def n = new NestedModule().addCommands(new Hello, new Ping)
      def m = new MyModule().addCommands(n)

      debug("display help of a sub command")
      m.execute("nest -h")
      
      debug("launch a command in a sub module")
      m.execute("nest hello")

      debug("show help of a command in a sub module")
      m.execute("nest ping -h")
    }

  }
}