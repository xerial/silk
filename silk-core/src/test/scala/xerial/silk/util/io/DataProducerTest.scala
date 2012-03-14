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

package xerial.silk.util.io

import xerial.silk.util.SilkSpec
import java.io.{PrintWriter, BufferedInputStream, PrintStream, OutputStream}

//--------------------------------------
//
// DataProducerTest.scala
// Since: 2012/03/14 11:20
//
//--------------------------------------

object DataProducerTest {
  class Hello extends TextDataProducer {
    def produce(out: PrintWriter) = {
      out.print("hello world")
    }

  }
}

/**
 * @author leo
 */
class DataProducerTest extends SilkSpec {
  import DataProducerTest._

  "DataProducer" should {

    "create producer" in {
      val out = new Hello()
      val lines = out.lines.toArray
      lines.size must be (1)
      lines(0) must be ("hello world")
    }
    
  }
}