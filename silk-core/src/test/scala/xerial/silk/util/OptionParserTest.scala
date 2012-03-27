package xerial.silk
package util

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

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import lens.ObjectSchema

//--------------------------------------
//
// OptionParserTest.scala
// Since: 2012/01/10 13:43
//
//--------------------------------------

object OptionParserTest {

  private class Config(
    @option(symbol = "h", longName = "help", description = "display help messages")
    val displayHelp: Boolean = false,

    @option(symbol = "c", description = "compression level")
    val compressionLevel: Int  = 3,

    @argument(description = "input files")
    val inputFile: Array[String] = Array.empty
  )


  class ArgConfig(
                   @option(symbol = "h", longName = "help", description = "display help messages")
                   val displayHelp: Boolean = false
                   )


}

trait ConfigTrait {
  @option(symbol = "h", longName = "help", description = "display help messages")
  var displayHelp: Boolean = false

  @option(symbol = "c", description = "compression level")
  var compressionLevel: Int = 2

  @argument(description = "input files")
  var inputFile: Array[String] = Array.empty
}

class ValConfig extends ConfigTrait

/**
 * @author leo
 */
@RunWith(classOf[JUnitRunner])
class OptionParserTest extends SilkSpec {

  import OptionParserTest._


  "OptionParser" should {

    "create option parsers" in {
      val p = OptionParser.of[Config]
    }


    "read option definitions from class definitions" in {
      val config: Config = OptionParser.parse[Config]("-h -c 10 file1 file2")
      config.displayHelp should be(true)
      config.compressionLevel should be(10)
      //debug {"input file option:" + config.inputFile.mkString(", ")}
      config.inputFile.size should be(2)
      config.inputFile(0) should be("file1")
      config.inputFile(1) should be("file2")
    }

    "create help messages" in {
      OptionParser.of[Config].printUsage
    }

    "detect option defined in extended trait" in {
      //pending
      val schema = ObjectSchema(classOf[ValConfig])
      val p = schema.parameters
      debug("params:%s", p.mkString(", "))
      p.length must be (3)

      val config = OptionParser.parse[ValConfig]("-h -c 3 f1 f2 f3")
      config.displayHelp should be(true)
      config.compressionLevel should be(3)
      config.inputFile.size should be(3)
      config.inputFile(0) should be("f1")
      config.inputFile(1) should be("f2")
      config.inputFile(2) should be("f3")
    }

    "detect option in constructor args" in {
      val config = OptionParser.parse[ArgConfig]("-h")
      config.displayHelp should be(true)
    }

    "be able to configure help message" in {
      val opt = OptionParser.of[Config]
      val usage = opt.createUsage()
      debug {
        usage
      }
    }
  }

  "TypeUtil" should {

    "detect types that can be created from buffer" in {
      TypeUtil.canBuildFromBuffer(TypeUtil.toClassManifest(java.lang.Integer.TYPE)) must be (false)
    }

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
        args.mkString(", ")
      }
      args should equal(Array("-c", "hello world!", "-f", "3.432"))
    }

  }

}