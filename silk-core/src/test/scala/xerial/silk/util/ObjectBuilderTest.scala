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
// ObjectBuilderTest.scala
// Since: 2012/01/25 12:56
//
//--------------------------------------

object ObjectBuilderTest {

  class ImObj(val i:Int=1, val s:String="hello")
  class PartialObj(val id:Int, val name:String="leo")

}

/**
 * @author leo
 */
class ObjectBuilderTest extends SilkWordSpec {

  import ObjectBuilderTest._

  "ObjectBuilder" should {

    "create builders of immutable objects" in {
      val b = ObjectBuilder(classOf[ImObj])


    }


  }

}