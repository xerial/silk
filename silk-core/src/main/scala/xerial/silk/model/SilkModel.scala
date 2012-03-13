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

package xerial.silk.model

import xerial.silk.util.CName

//--------------------------------------
//
// SilkModel.scala
// Since: 2012/01/24 15:04
//
//--------------------------------------

/**
 * @author leo
 */
object SilkModel {
  val primitiveTypes = Seq(SilkByte, SilkShort, SilkInteger, SilkLong, SilkBoolean, SilkFloat, SilkDouble, SilkString, SilkOption)
}

trait SilkType {
  def signature: String 
}
abstract class SilkPrimitive(val name:String, val alias:Array[String]) extends SilkType {
  def signature = name
}

case class SilkByte extends SilkPrimitive("byte", Array("int8"))
case class SilkShort extends SilkPrimitive("short", Array("int16"))
case class SilkInteger extends SilkPrimitive("integer", Array("int32", "int"))
case class SilkLong extends SilkPrimitive("long", Array("int64"))
case class SilkBoolean extends SilkPrimitive("boolean", Array("bool"))
case class SilkFloat extends SilkPrimitive("float", Array("real32"))
case class SilkDouble extends SilkPrimitive("double", Array("real64", "real"))
case class SilkString extends SilkPrimitive("string", Array("text"))

/**
 * Optional type
 * @param elementType
 */
case class SilkOption(elementType:SilkType) extends SilkType {
  override def signature = "option[%s]".format(elementType.signature)
}

/**
 * A type for long-running list of elements
 * @param elementType
 */
case class SilkStream(elementType:SilkType) extends SilkType {
  def signature = "stream[%s]".format(elementType.signature)
}


/**
 * Ordered elements
 * @param elementType
 */
case class SilkArray(elementType:SilkType) extends SilkType {
  def signature = "array[%s]".format(elementType.signature)
}


/**
 * Unordered set
 * @param elementType
 */
case class SilkSet(elementType:SilkType) extends SilkType {
  def signature = "set[%s]".format(elementType.signature)
}

/**
 * Map
 * @param keyType
 * @param valueType
 */
case class SilkMap(keyType:SilkType,  valueType:SilkType) extends SilkType {
  def signature = "map[%s,%s]".format(keyType.signature, valueType.signature)
}

/**
 * A type for representing complex records
 * @param name
 * @param params
 */
case class SilkRecord(name:String, params:Array[SilkType]) extends SilkType {
  val cname = CName(name)
  def signature = "%s(%s)".format(cname, params.map(_.signature).mkString(","))
}



