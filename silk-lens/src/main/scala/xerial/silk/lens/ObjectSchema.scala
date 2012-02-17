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
import java.lang.reflect.Field
import xerial.silk.util.{StringTemplate, TypeUtil}

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
  
  def getSchemaOf(obj:Any) : ObjectSchema = apply(obj.getClass)

  /**
   * Get the object schema of the specified type. This method caches previously created ObjectSchema instances, and
   * second call for the same type object return the cached entry.
   */
  def apply(cl:Class[_]) : ObjectSchema = schemaTable.getOrElseUpdate(cl, new ObjectSchema(cl)) 

  class Attribute(val name: String, val valueType: Class[_]) {
    override def toString = "%s:%s".format(name, valueType.getSimpleName)
  }

}


/**
 * Information of object parameters and their s
 * @author leo
 */
class ObjectSchema(val cl: Class[_]) {

  import ObjectSchema._


  private def lookupAttributes = {
    // filter internal scala fields
    for (f <- cl.getDeclaredFields; if !f.getName.startsWith("$")) yield {
      new Attribute(f.getName, f.getType)
    }
  }

  val name: String = cl.getSimpleName
  val fullName : String = cl.getName
  val attributes: Array[Attribute] = lookupAttributes
  private val attributeIndex : Map[String, Attribute] = {
    val pair = for(a <- attributes) yield a.name -> a
    pair.toMap
  }

  def getAttribute(name:String) : Attribute = {
    attributeIndex(name)
  }

  /**
   * Read the object parameter by using reflection
   */
  def read(obj:Any, attribute:Attribute) : Any = {
    val f : Field = obj.getClass.getDeclaredField(attribute.name)
    val v = TypeUtil.readField(obj, f)
    v
  }
  
  override def toString = {
    val b = new StringBuilder
    b append (cl.getSimpleName + "(")
    b append (attributes.mkString(", "))
    b.append (")")
    b.toString
  }
}