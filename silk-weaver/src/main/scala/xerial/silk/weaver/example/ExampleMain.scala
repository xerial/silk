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

import xerial.lens.cui.{argument, command, option}
import xerial.core.log.{LogLevel, Logger}
import scala.sys.process.Process
import xerial.silk.weaver.DefaultMessage
import scala.util.Random
import xerial.core.util.{DataUnit, Timer}
import java.io.File


case class Person(id:Int, name:String) {
  def toTSV : String = s"$id\t$name"
}

object Person {
  implicit object PersonOrdering extends Ordering[Person] {
    def compare(x: Person, y: Person) = {
      val diff = x.id - y.id
      if(diff != 0)
        diff
      else
        x.name.compareTo(y.name)
    }
  }

  def randomName = {
    val s = new StringBuilder
    for(i <- 0 to 3 +Random.nextInt(7)) s.append(('a' + Random.nextInt(25)).toChar)
    s.result
  }
}

import xerial.silk._
import xerial.silk.cluster._

/**
 * @author Taro L. Saito
 */
class ExampleMain(@option(prefix = "-z", description = "zk connect string")
                  zkConnectString:String = config.zk.zkServersConnectString)
  extends DefaultMessage with Timer with Logger {

  @command(description = "Execute a command in remote machine")
  def remoteFunction(@option(prefix = "--host", description = "hostname")
                     hostName: Option[String] = None) {

    if (hostName.isEmpty) {
      warn("No hostname is given")
      return
    }


    val h = hosts.find(_.name == hostName.get)
    at(h.get) {
      println(Process("hostname").!!)
    }
  }

  @command(description = "Sort data set")
  def sort(@option(prefix = "-N", description = "num entries")
           N: Int = 1024 * 1024,
           @option(prefix = "-m", description = "num mappers")
           M: Int = defaultHosts().size * 2,
           @option(prefix = "-r", description = "num reducers")
           numReducer: Int = 3
            ) {

    silkEnv(zkConnectString) {
      // Create a random Int sequence
      time("distributed sort", logLevel = LogLevel.INFO) {

        info("Preparing random data")
        val B = (N.toDouble / M).ceil.toInt

        info(f"N=$N%,d, B=$B%,d, M=$M")
        val seed = Silk.scatter((0 until M).toIndexedSeq, M)
        val input = seed.fMap{s =>
          val numElems = if(s == (M-1) && (N % B != 0)) N % B else B
          (0 until numElems).map(x => Random.nextInt(N))
        }
        val sorted = input.sorted(new RangePartitioner(numReducer, input))
        val result = sorted.get
        info(s"sorted: ${result.size} [${result.take(10).mkString(", ")}, ...]")

      }
    }
  }

  @command(description = "Sort objects")
  def objectSort(@option(prefix = "-N", description = "num entries")
           N: Int = 1024 * 1024,
           @option(prefix = "-m", description = "num mappers")
           M: Int = defaultHosts().size * 2,
           @option(prefix = "-r", description = "num reducers")
           R: Int = 3) {

    silkEnv(zkConnectString) {
      // Create a random Int sequence
      time("distributed sort", logLevel = LogLevel.INFO) {

        info("Preparing random data")
        val B = (N.toDouble / M).ceil.toInt
        info(f"N=$N%,d, B=$B%,d, M=$M")

        val seed = Silk.scatter((0 until M).toIndexedSeq, M)
        val input = seed.fMap{s =>
          val numElems = if(s == (M-1) && (N % B != 0)) N % B else B
          (0 until numElems).map(x => new Person(Random.nextInt(N), Person.randomName))
        }
        val sorted = input.sorted(new RangePartitioner(R, input))
        val result = sorted.eval
        val resultSize = result.size.get
        info(s"sorted: ${resultSize}") // [${result.take(10).mkString(", ")}, ...]")
      }
    }

  }

  @command(description = "Load a file and split the lines by tab")
  def loadFile(@argument(description="input file") file:String) {

    silkEnv(zkConnectString) {
      time("split tab-separted data", logLevel=LogLevel.INFO) {
        val f = Silk.loadFile(file)
        val columns = for(line <- f.lines) yield line.split("""\t""")
        val numLines = columns.eval.size.get
        info(f"parsed $numLines%,d lines")
      }
    }


  }

}