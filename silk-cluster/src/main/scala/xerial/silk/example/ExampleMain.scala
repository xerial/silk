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

//--------------------------------------
//
// ExampleMain.scala
// Since: 2012/12/20 5:32 PM
//
//--------------------------------------

package xerial.silk.example

import xerial.silk.DefaultMessage
import xerial.lens.cui.{command, option}
import xerial.core.log.Logger
import scala.sys.process.Process

/**
 * @author Taro L. Saito
 */
class ExampleMain extends DefaultMessage with Logger {

  import xerial.silk._
  import xerial.silk.cluster._

  @command(description = "Execute a command in remote machine")
  def remoteFunction(@option(prefix="--host", description="hostname")
                    hostName:Option[String] = None) {

    if(hostName.isEmpty) {
      warn("No hostname is given")
      return
    }


    val h = hosts.find(_.name == hostName.get)
    at(h.get) {
      println(Process("hostname").!! )
    }
  }


}