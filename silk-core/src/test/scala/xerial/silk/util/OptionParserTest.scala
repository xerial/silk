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

class Config {
  @option(symbol = "h", description = "display help messages")
  var displayHelp: Boolean = false

  @argument(description = "input files")
  var inputFile: Array[String] = Array.empty
}

/**
 * @author leo
 */
@RunWith(classOf[JUnitRunner])
class OptionParserTest extends SilkSpec {


  "option parser" should "read options from class definitions" in {

    val config: Config = OptionParser.parse(classOf[Config], "-h file1 file2".split("\\s+"))
    config.displayHelp should be(true)
    config.inputFile.size should be(2)
    config.inputFile(0) should be("file1")
    config.inputFile(1) should be("file2")
  }

}