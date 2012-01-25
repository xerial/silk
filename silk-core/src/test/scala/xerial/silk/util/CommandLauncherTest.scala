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
// CommandLauncherTest.scala
// Since: 2012/01/25 10:21
//
//--------------------------------------

/**
 * @author leo
 */
class CommandLauncherTest extends SilkWordSpec {


  "Command" should {
    class MyCommand extends Command[Unit] {

      def name = "hello"

      def oneLineDescription = "display hello"

      def helpHeader = """$ hello
  display hello"""

      var executed = false

      def execute() {
        debug("hello world")
        executed = true
      }
    }
    "be created by extending Command trait" in {
      val c = new MyCommand
      c.execute(Array[String]())
      c.executed must be(true)
    }

    "set options" in {
      val c = new MyCommand { override val optionHolder = new {
        @option(symbol = "h", description = "display help message")
        var displayHelp : Boolean = false
      }}

      c.execute()
      c.optionHolder.displayHelp should be (true)
    }

  }


  "CommandLauncher" should {

    "invoke a command" in {

    }

  }

}