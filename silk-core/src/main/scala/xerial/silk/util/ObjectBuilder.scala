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

import java.lang.reflect.Field
import scala.collection.mutable.Map

//--------------------------------------
//
// ObjectBuilder.scala
// Since: 2012/01/25 12:41
//
//--------------------------------------

/**
 *
 *
 */
object ObjectBuilder {

  // class ValObj(val p1, val p2, ...)
  // class VarObj(var p1, var p2, ...)

  def apply[A](cl:Class[A]) : ObjectBuilder[A] = {

    def isValidField(f:Field) : Boolean = {
      val t = f.getType
      TypeUtil.canInstantiate(t)
    }

    if(!TypeUtil.canInstantiate(cl))
      throw new IllegalArgumentException("Cannot instantiate class " + cl)

    val field = for(f <- cl.getDeclaredFields; if isValidField(f)) yield f
    val defaultValue = TypeUtil.defaultConstructorParameters(cl)

    val prop = Map[String, Any]()
    for((f, i) <- field.zipWithIndex) {
      prop += f.getName -> defaultValue(i) 
    }


    new ObjectBuilderFromString(cl, field, prop)
  }


}

/**
 * Generic object builder
 * @author leo
 */
trait ObjectBuilder[A] {

  def get(name:String) : Option[_]
  def set(name:String, value:Any) : Unit
  def build : A
}


class ObjectBuilderFromString[A](cl:Class[A], field:Array[Field], prop:Map[String, Any]) extends ObjectBuilder[A] {

  def get(name: String) = {
    prop.get(name)
  }

  def set(name: String, value: Any) = {
    prop += name -> value
  }

  def build = {
    val constructorArgs = (for(f <- field) yield {
      val v = prop.getOrElse(f.getName, TypeUtil.zero(f.getType))
      v.asInstanceOf[AnyRef]
    }).toSeq
    
    TypeUtil.newInstance(cl, constructorArgs).asInstanceOf[A]
  }
}