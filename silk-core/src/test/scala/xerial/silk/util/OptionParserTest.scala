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

  class ValConfig
  (
    @option(symbol = "h", longName = "help", description = "display help messages")
    val displayHelp: Boolean = true,

    @option(symbol = "c", description = "compression level")
    val compressionLevel: Int,

    @argument(description = "input files")
    val inputFile: Array[String] = Array.empty
    )

}

/**
 * @author leo
 */
@RunWith(classOf[JUnitRunner])
class OptionParserTest extends SilkSpec {

  import OptionParserTest._

  "option parser" should "read options from class definitions" in {
    val c = classOf[Config].getConstructor().newInstance()

    val config: Config = OptionParser.parse(classOf[Config], "-h -c 10 file1 file2".split("\\s+"))
    config.displayHelp should be(true)
    config.inputFile.size should be(2)
    config.compressionLevel should be(10)
    config.inputFile(0) should be("file1")
    config.inputFile(1) should be("file2")
  }

  "option parser" should "create help message" in {
    OptionParser.displayHelpMessage(classOf[Config])
    OptionParser.displayHelpMessage(classOf[ValConfig])
  }

  "option parser" should "detect val fields" in {
    val config = OptionParser.parse(classOf[ValConfig], "-h -c 3 f1 f2 f3".split("\\s+"))
    config.displayHelp should be(true)
    config.inputFile.size should be(2)
    config.compressionLevel should be(10)
    config.inputFile(0) should be("file1")
    config.inputFile(1) should be("file2")
  }


  "option parser" should "report an error when using inner classes" in {
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
      val v = OptionParser.newInstance(classOf[A])
    }
  }

  "CommandLine" should "be tokenized" in {
    import OptionParser.CommandLineTokenizer
    val args = CommandLineTokenizer.tokenize("""-c "hello world!" -f 3.432""")

    args.length must be (4)
    debug { args.toString }
    args(0) should equal ("-c")
    args(1) should equal ("hello world!")
    args(2) should equal ("-f")
    args(3) should equal ("3.432")
  }

}

