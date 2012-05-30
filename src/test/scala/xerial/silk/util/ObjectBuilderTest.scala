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
class ObjectBuilderTest extends SilkSpec {

  import ObjectBuilderTest._

  "ObjectBuilder" can {

    "create builders of immutable objects" in {
      val b = ObjectBuilder(classOf[ImObj]).build

      b.i must be (1)
      b.s must be ("hello")
    }
    
    "set parameter values" in {
      val b = ObjectBuilder(classOf[ImObj])
      b.set("i", 3242)
      
      val bi = b.build
      bi.i must be (3242)
      
      b.set("s", "hello world")
      val bi2 = b.build
      bi2.i must be (3242)
      bi2.s must be ("hello world")
    }

    "create classes with default values for some parameters" in {
      val b = ObjectBuilder(classOf[PartialObj])
      val p = b.build

      p.id should be (0)
      p.name must be ("leo")
    }



  }

}