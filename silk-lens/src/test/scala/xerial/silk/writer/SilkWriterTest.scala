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

package xerial.silk.writer

import xerial.silk.util.SilkSpec

//--------------------------------------
//
// SilkWriterTest.scala
// Since: 2012/01/17 13:52
//
//--------------------------------------

/**
 * @author leo
 */
class SilkWriterTest extends SilkSpec {

  class A(val id:Int, val name:String)

  "SilkTextWriter" should "output class contents" in {
    val a = new A(1, "leo")
    val silk = SilkTextWriter.toSilk(a)
    debug { silk }
  }

  "SilkTextWriter" should "output array value" in {
    val a = Array(0, 1, 10, 50)
    val silk = SilkTextWriter.toSilk(a)
    debug { silk }
  }


}