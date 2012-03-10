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
// ShellTest.scala
// Since: 2012/02/06 13:04
//
//--------------------------------------

/**
 * @author leo
 */
class ShellTest extends SilkSpec {
  "Shell" should {
    "find JVM" in {
      val cmd = Shell.findJavaCommand()
      debug("JAVA_HOME:%s", System.getenv("JAVA_HOME"))
      debug(cmd)
    }

    "find javaw.exe" in {
      if (OS.isWindows) {
        when("OS is windows")
        val cmd = Shell.findJavaCommand("javaw").get
        cmd must not be (null)
        cmd must include("javaw")
      }

    }

    "be able to launch Java" in {
      Shell.launchJava("-version -Duser.language=en")
    }


    "find sh" in {
      val cmd = Shell.findSh

    }

    "launch command" in {
      Shell.launchProcess("echo hello world")
      Shell.launchProcess("echo cygwin env=$CYGWIN")
    }

    "launch process" in {
      if (OS.isWindows) {
        when("OS is windows")
        Shell.launchCmdExe("echo hello cmd.exe")
      }
    }
  }


}