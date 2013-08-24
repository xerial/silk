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

import xerial.lens.cui.{command, option}
import xerial.core.log.Logger
import scala.sys.process.Process
import xerial.silk.weaver.DefaultMessage
import scala.util.Random
import xerial.core.util.Timer

/**
 * @author Taro L. Saito
 */
class ExampleMain extends DefaultMessage with Timer with Logger {

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

  @command(description = "Sort data set")
  def sort(@option(prefix="-s", description="num splits") splits:Int=6,
           @option(prefix="-z", description="zk connect string")
           zkConnectString:String = config.zk.zkServersConnectString,
           @option(prefix="-N", description="data size")
           N:Int = 1000000,
           @option(prefix="-r", description="num reducers")
           numReducer:Int = 3
            ) {

    silkEnv(zkConnectString) {
      // Create a random Int sequence
      info("Preapring random data")
      val data = for(i <- (0 until N).par) yield Random.nextInt(N)
      info("Scattering data to remote node")

      val t = time("distributed sort") {
        val input = Silk.scatter(data.seq, splits)
        val sorted = input.sorted(new RangePartitioner(numReducer, input))
        val result = sorted.get
        info(s"sorted: ${result.size} [${result.take(10).mkString(", ")}, ...]")
        result
      }
      info(t)
    }

  }


}