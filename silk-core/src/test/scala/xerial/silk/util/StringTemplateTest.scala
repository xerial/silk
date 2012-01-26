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


//--------------------------------------
//
// StringTemplateTest.scala
// Since: 2012/01/16 11:03
//
//--------------------------------------

/**
 * @author leo
 */
class StringTemplateTest extends SilkFlatSpec {

  import StringTemplate._

  "template" should "replace variables" in {
    val template ="""hello $WORLD$"""
    val s = eval(template)(Map("WORLD" -> "world!"))

    s should be("hello world!")
  }

  "template" should "accept symbol key" in {
    val template ="""hello $WORLD$"""
    val s = eval(template)(Map('WORLD -> "world!"))

    s should be("hello world!")
  }

}