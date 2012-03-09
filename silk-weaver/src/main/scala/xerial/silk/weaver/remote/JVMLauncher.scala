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

import sys.process.Process
import xerial.silk.util._
import java.lang.{IllegalStateException}
import xerial.silk.opt.CommandLineTokenizer

//--------------------------------------
//
// JVMLauncher.scala
// Since: 2012/01/30 11:51
//
//--------------------------------------

/**
 * Launches Java VM for multi-JVM task execution
 * @author leo
 */
object JVMLauncher extends Logging {

  def launchJava(args: String) = {
    val javaCmd = Shell.findJavaCommand()
    if (javaCmd.isEmpty)
      throw new IllegalStateException("No JVM is found. Set JAVA_HOME environmental variable")

    val cmdLine = "%s %s".format(javaCmd.get, args)

    debug("Run command: " + cmdLine)

    Process(CommandLineTokenizer.tokenize(cmdLine)).run()
  }


}