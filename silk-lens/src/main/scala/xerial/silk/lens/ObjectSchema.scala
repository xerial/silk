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

import collection.mutable.WeakHashMap

//--------------------------------------
//
// ObjectSchema.scala
// Since: 2012/01/17 10:05
//
//--------------------------------------

/**
 *
 */
object ObjectSchema {

  private val schemaTable = new WeakHashMap[Class[_], ObjectSchema]


  def getSchemaOf[A](obj:A) : ObjectSchema = get(obj.getClass)

  /**
   * Get the object schema of the specified type. This method caches previously created ObjectSchema instances, and
   * second call for the same type object return the cached entry.
   */
  def get[A](cl:Class[A]) : ObjectSchema = {
    if(schemaTable.isDefinedAt(cl))
      schemaTable(cl).asInstanceOf[ObjectSchema]
    else {
      val newSchema = new ObjectSchema(cl)
      schemaTable += cl -> newSchema
      newSchema
    }
  }

  class Attribute(val name: String, val valueType: Class[_]) {
    override def toString = "%s:%s".format(name, valueType.getName)
  }



}


/**
 * Information of object parameters and their types
 * @author leo
 */
class ObjectSchema(cl: Class[_]) {

  import ObjectSchema._
  private def lookupAttributes = {
    // filter internal scala fields
    for (f <- cl.getDeclaredFields; if !f.getName.startsWith("$")) yield {
      new Attribute(f.getName, f.getType)
    }
  }

  def name: String = cl.getName
  val attributes: Array[Attribute] = lookupAttributes
  def baseClass : Class[_] = cl
  
}