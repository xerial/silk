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

package xerial.silk.lens

import xerial.silk.util.SilkSpec

//--------------------------------------
//
// ObjectLensTest.scalaince: 2012/02/09 16:42
//
//--------------------------------------

object ObjectLensTest {
  case class Person(id:Int, name:String)
  case class PersonList(person:Seq[Person])
}

/**
 * @author leo
 */
class ObjectLensTest extends SilkSpec {

  import ObjectLensTest._

  "ObjectLens" should {
    "convert objects into Silk" in {
      val p = new Person(1, "leo")
      val silk = ObjectLens.toSilk(p)

      debug { "silk: " + silk }
    }

    "update fields in objects using Silk" in {
      val silk = """(id:1, name:leo)"""
      val p : Person = ObjectLens.updateWithSilk(Person(1, "leo"), silk)

      debug { "person: " + p }
    }

    "create objects from Silk" in {
      val silk = """(id:1, name:leo)"""
      val p = ObjectLens.createFromSilk(classOf[Person], silk)
      debug { "person: " + p }
    }

  }
}