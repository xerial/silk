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

import sys.SystemProperties
import sys.process.Process
import java.io.File
import xerial.silk.util.{Logging, OS, OSInfo, CommandLineTokenizer}

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
    val javaCmd = findJavaCommand
    val cmdLine =  "%s %s".format(javaCmd, args)

    debug("command: " + cmdLine)

    Process(cmdLine).run()
  }

  def findJavaCommand: String = {

    def javaBin(java_home: String) = java_home + "/bin/java" + (if (OSInfo.isWindows) ".exe" else "")

    def hasJavaCommand(java_home: String): Boolean = {
      val java_path = new File(javaBin(java_home))
      java_path.exists()
    }

    def lookupJavaHome : Option[String] = {
      val e = System.getenv("JAVA_HOME")
      debug("Found JAVA_HOME:" + e)
      if (e == null)
        None

      import OS._
      OSInfo.getOSType match {
        case Windows => {
          val m = """/cygdrive/(\w)(/.*)""".r.findFirstMatchIn(e)
          if (m.isDefined)
            Some("%s:%s".format(m.get.group(1), m.get.group(2)))
          else
            Some(e)
        }
        case _ => Some(e)
      }
    }

    val java_home: Option[String] = {
      val e = lookupJavaHome

      import OS._
      e match {
        case Some(x) => e
        case None => {
          def listJDKIn(path: String) = {
            // TODO Oracle JVM (JRockit) support
            new File(path).listFiles().
              filter(x => x.isDirectory
              && (x.getName.startsWith("jdk") || x.getName.startsWith("jre"))
              && hasJavaCommand(x.getAbsolutePath)).map(_.getAbsolutePath)
          }
          def latestJDK(jdkPath: Array[String]): Option[String] = {
            if (jdkPath.isEmpty)
              None
            else {
              // TODO parse version number
              val sorted = jdkPath.sorted.reverse
              Some(sorted(0))
            }
          }
          debug("No java command found. Searching for JDK...")


          OSInfo.getOSType match {
            case Windows => latestJDK(listJDKIn("c:/Program Files/Java"))
            case Mac => {
              val l = Seq("/System/Library/Frameworkds/JavaVM.framework/Home", "/System/Library/Frameworkds/JavaVM.framework/Versions/CurrentJDK/Home").
                filter(hasJavaCommand)
              if (l.isEmpty)
                None
              else
                Some(l(0))
            }
            case _ => None
          }
        }
      }
    }

    val java_bin = java_home match {
      case Some(x) => javaBin(x)
      case None => Process("which java") !!
    }

    java_bin.trim()
  }

}