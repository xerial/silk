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
import java.lang.reflect.ParameterizedType

//--------------------------------------
//
// ObjectSchemaTest.scala
// Since: 2012/01/17 10:17
//
//--------------------------------------

object ObjectSchemaTest {
  class A(val id:Int, val name:String)
  class B(var flag:Option[Boolean])
}

/**
 * @author leo
 */
class ObjectSchemaTest extends SilkSpec {
  import ObjectSchemaTest._

  "ObjectSchema" should {
    "enumerate all fields" in {

      val s = new ObjectSchema(classOf[A])
      debug { s.toString }
      s.name must be (classOf[A].getSimpleName)
      s.fullName must be (classOf[A].getName)

      val attr = s.attributes
      attr.length must be (2)
      attr(0).name must be ("id")
      attr(0).valueType must be (classOf[Int])
      attr(1).name must be ("name")
      attr(1).valueType must be (classOf[String])

    }

    "lookup option fields" in {
      val b = new B(Some(true))

      val s = ObjectSchema(classOf[B])
      debug(s)
      val t = s.getAttribute("flag").valueType.getGenericInterfaces
      
      
      debug { classOf[B].getDeclaredField("flag").getGenericType.asInstanceOf[ParameterizedType].getActualTypeArguments()(0) }
      
    }

  }
}