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

package xerial.silk.text.parser

import xerial.silk.util.SilkSpec
import xerial.silk.parser.SilkSample

//--------------------------------------
//
// SilkParserTest.scala
// Since: 2012/08/10 16:05
//
//--------------------------------------

/**
 * @author leo
 */
class SilkParserTest extends SilkSpec {


  import SilkSample._
  import SilkParser._
  import SilkExpr._
  import Token._

  def e(expr:Expr[SilkToken], silk:String) {
    val r = SilkParser.parse(expr, silk)
    debug(r)
  }

  "SilkParser" should {
    "parse preambles" in {
      e(preamble, p0)
      e(preamble, p1)
    }

  }
}