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

package xerial.silk.remote

import xerial.silk.util.{OSInfo, SilkSpec}


//--------------------------------------
//
// JVMLauncherTest.scala
// Since: 2012/01/30 12:55
//
//--------------------------------------

/**
 * @author leo
 */
class JVMLauncherTest extends SilkSpec {

  "JVMLauncher" should {
    "find JVM" in {
      val cmd = JVMLauncher.findJavaCommand()
      debug("JAVA_HOME:%s", System.getenv("JAVA_HOME"))
      debug(cmd)
    }

    "be able to launch Java" in {
      JVMLauncher.launchJava("-version -Duser.language=en")
    }


    "find javaw.exe" in {
      when("OS is windows")
      if (OSInfo.isWindows) {
        val cmd = JVMLauncher.findJavaCommand("javaw").get
        cmd must not be (null)
        cmd must include("javaw")
      }

    }

    "find sh" in {
      val cmd = JVMLauncher.findSh

    }

    "launch command" in {
      JVMLauncher.launchProcess("echo hello world")
      JVMLauncher.launchProcess("echo cygwin env=$CYGWIN")
    }

    "launch process" in {
      when("OS is windows")
      if (OSInfo.isWindows) {
        JVMLauncher.launchProcessInWindows("echo hello cmd.exe")
      }
    }
  }
}