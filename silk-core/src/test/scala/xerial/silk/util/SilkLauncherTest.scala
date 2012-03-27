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

import io.DataProducer
import scala.io.Source
import java.io.{PrintStream, OutputStream, InputStream}

//--------------------------------------
//
// SilkLauncherTest.scala
// Since: 2012/03/22 14:31
//
//--------------------------------------

object SilkLauncherTest {

  class TestModule extends SilkCommandModule with Logger {
    val moduleName = "global"

    @command(description="say hello")
    def hello = "Hello World"

    @command(description="do ping-pong")
    def ping(@argument name:String="pong")
    = "ping %s".format(name)

  }

  class SubModule {
    def waf = "Waf! Waf!"
  }

  class StreamTestModule extends SilkCommandModule with Logger {
    val moduleName = "stream test"

    @command(description="process stream")
    def stream(in:InputStream) = {
      for(line <- Source.fromInputStream(in).getLines()) {
        debug(line)
      }
    }

  }

  
}


/**
 * @author leo
 */
class SilkLauncherTest extends SilkSpec {

  import SilkLauncherTest._
  
  "SilkLauncher" should {
    "detect global option" in {
      SilkLauncher.of[TestModule].execute("-h")
      SilkLauncher.of[TestModule].execute("ping -h")


    }

    "call method without arguments" in {
      val ret = SilkLauncher.of[TestModule].execute("hello")
      ret must be ("Hello World")
    }

    "call method with arguments" in {
      val ret = SilkLauncher.of[TestModule].execute("ping pong")
      ret must be ("ping pong")
    }

    "use default method argumets" in {
      val ret = SilkLauncher.of[TestModule].execute("ping")
      ret must be ("ping pong")
    }

    "pass stream input" in {
      pending
      val l = SilkLauncher.of[StreamTestModule]
      l.execute(new DataProducer() {
        def produce(out: OutputStream) {
          val p = new PrintStream(out)
          p.println("hello world!")
          p.flush
        }
      })
    }

  }

}