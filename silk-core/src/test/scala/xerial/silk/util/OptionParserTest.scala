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

package xerial.silk
package util

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

//--------------------------------------
//
// OptionParserTest.scala
// Since: 2012/01/10 13:43
//
//--------------------------------------

object OptionParserTest {

  private class Config {
    @option(symbol = "h", longName = "help", description = "display help messages")
    var displayHelp: Boolean = false

    @option(symbol = "c", description = "compression level")
    var compressionLevel = 3

    @argument(description = "input files")
    var inputFile: Array[String] = Array.empty
  }

  trait ValConfig
  {
    @option(symbol = "h", longName = "help", description = "display help messages")
    val displayHelp: Boolean = true

    @option(symbol = "c", description = "compression level")
    val compressionLevel: Int

    @argument(description = "input files")
    val inputFile: Array[String] = Array.empty
  }

}

/**
 * @author leo
 */
@RunWith(classOf[JUnitRunner])
class OptionParserTest extends SilkSpec {

  import OptionParserTest._


  "OptionParser" should {

    "create option parsers" in {
      OptionParser(classOf[Config])
    }


    "read option definitions from class definitions" in {
      val c = classOf[Config].getConstructor().newInstance()

      val config: Config = OptionParser.parse(classOf[Config], "-h -c 10 file1 file2")
      config.displayHelp should be(true)
      config.compressionLevel should be(10)
      config.inputFile.size should be(2)
      config.inputFile(0) should be("file1")
      config.inputFile(1) should be("file2")
    }

    "create help messages" in {
      OptionParser(classOf[Config]).printUsage
      //OptionParser.displayHelpMessage(classOf[ValConfig])
    }

    "detect val fields" in {
      pending
      val config = OptionParser.parse(classOf[ValConfig], "-h -c 3 f1 f2 f3")
      config.displayHelp should be(true)
      config.inputFile.size should be(2)
      config.compressionLevel should be(10)
      config.inputFile(0) should be("file1")
      config.inputFile(1) should be("file2")
    }
  }

  "TypeUtil" should {
    "report an error when using inner classes" in {
      class A
      (
        @option(symbol = "h", longName = "help", description = "display help messages")
        val displayHelp: Boolean = true,

        @option(symbol = "c", description = "compression level")
        val compressionLevel: Int,

        @argument(description = "input files")
        val inputFile: Array[String] = Array.empty
        )

      intercept[IllegalArgumentException] {
        val v = TypeUtil.newInstance(classOf[A])
      }
    }
  }

  "CommandLine" should {
    "tokenize a single string into args" in {
      val args = CommandLineTokenizer.tokenize("""-c "hello world!" -f 3.432""")

      args.length must be(4)
      debug {
        args.toString
      }
      args should equal (Array("-c", "hello world!", "-f", "3.432"))
    }

  }

}