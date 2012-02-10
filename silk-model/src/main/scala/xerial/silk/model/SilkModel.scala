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
import scala.collection.mutable.Map
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


}

object SilkValueType {

  private val typeNameTable = Map[String, SilkValueType]()

  val primitiveTypes = Seq(silkInt, silkShort, Long, Boolean, Float, Double, Char, Byte, String)
  
  val silkInt = new SilkValueType("int", Seq("integer", "int32"))
  val silkShort = new SilkValueType("short", Seq("int16"))
  val Long = new SilkValueType("long", Seq("int64"))
  val Boolean = new SilkValueType("boolean", Seq("bool"))
  val Float = new SilkValueType("float", Seq("real32"))
  val Double = new SilkValueType("double", Seq("real", "real64"))
  val Char = new SilkValueType("char", Seq.empty)
  val Byte = new SilkValueType("byte", Seq("int8"))
  val String = new SilkValueType("string", Seq("text"))

  // Init the type table
  for (t <- primitiveTypes) {
    typeNameTable += t.name -> t
    for (a <- t.alias)
      typeNameTable += a -> t
  }



}

class SilkValueType(val name: String, val alias: Seq[String]) {


}