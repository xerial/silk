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
  val PrimitiveTypes: Seq[Class[_]] =
    Seq(classOf[SilkByte], classOf[SilkShort], classOf[SilkInteger],
      classOf[SilkLong], classOf[SilkBoolean], classOf[SilkFloat],
      classOf[SilkDouble], classOf[SilkString], classOf[SilkOption],
      classOf[SilkAlnum], classOf[SilkEnum])

  val ReservedName = Seq(
    // reserved node names
    "_root",
    // reserved key words
    "import", "include", "module", "package",
    // primitive types
    "byte", "int8",
    "short", "int16",
    "integer", "int32", "int",
    "long", "int64",
    "boolean", "bool",
    "float", "real32",
    "double", "real64", "real",
    "string", "text",
    // enhanced types
    "alnum", "enum", "option",
    // data structure types
    "stream", "array", "list", "set", "map",
    // complex objects,
    "record", "tuple"
  )

}


/**
 * Base trait of silk types
 */
trait SilkType {
  def signature: String
}

/**
 * Base trait of all types that can be a value
 */
trait SilkValueType extends SilkType

/**
 * Base trait of all primitive types
 * @param name
 * @param alias
 */
abstract class SilkPrimitive(val name: String, val alias: Array[String]) extends SilkValueType {
  def signature = name
}

class SilkByte extends SilkPrimitive("byte", Array("int8"))

class SilkShort extends SilkPrimitive("short", Array("int16"))

class SilkInteger extends SilkPrimitive("integer", Array("int32", "int"))

class SilkLong extends SilkPrimitive("long", Array("int64"))

class SilkBoolean extends SilkPrimitive("boolean", Array("bool"))

class SilkFloat extends SilkPrimitive("float", Array("real32"))

class SilkDouble extends SilkPrimitive("double", Array("real64", "real"))

class SilkString extends SilkPrimitive("string", Array("text"))

/**
 * Alpha-numeric types (e.g., "chr1", "chr2", .., "chr10", etc.),
 * which is ordered first by alphabet prefixes then next numeric values.
 */
class SilkAlnum extends SilkValueType {
  def signature = "alnum"
}

/**
 * Enumeration types of fixed number of values
 * @param name
 * @param values
 */
case class SilkEnum(name: String, values: Array[String]) extends SilkValueType {
  def signature = "enum(%s,[%s])".format(name, values.mkString(","))
}


/**
 * Optional type
 * @param elementType
 */
case class SilkOption(elementType: SilkType) extends SilkValueType {
  override def signature = "option[%s]".format(elementType.signature)
}

/**
 * A type for long-running list of elements. Indexed access is not supported in this type
 * @param elementType
 */
case class SilkStream(elementType: SilkType) extends SilkValueType {
  def signature = "stream[%s]".format(elementType.signature)
}


/**
 * Ordered elements with a support of indexed access
 * @param elementType
 */
case class SilkArray(elementType: SilkType) extends SilkValueType {
  def signature = "array[%s]".format(elementType.signature)
}

/**
 * Ordered element list. No indexed access is supported
 * @param elementType
 */
case class SilkList(elementType: SilkType) extends SilkValueType {
  def signature = "list[%s]".format(elementType.signature)
}

/**
 * Unordered set
 * @param elementType
 */
case class SilkSet(elementType: SilkType) extends SilkValueType {
  def signature = "set[%s]".format(elementType.signature)
}

/**
 * Map type
 * @param keyType
 * @param valueType
 */
case class SilkMap(keyType: SilkType, valueType: SilkType) extends SilkValueType {
  def signature = "map[%s,%s]".format(keyType.signature, valueType.signature)
}

/**
 * Named type is used for defining records
 * @param name
 * @param valueType
 */
case class SilkNamedType(name: String, valueType: SilkType) extends SilkType {
  def signature = "%s:%s".format(name, valueType.signature)
}

/**
 * Element of SilkSchema
 */
trait SilkSchemaElement extends SilkType

/**
 * A type for representing complex records
 * @param name
 * @param params
 */
case class SilkRecord(name: String, params: Array[SilkNamedType]) extends SilkValueType with SilkSchemaElement {
  val cname = CName(name)

  def signature = "record(%s,[%s])".format(cname, params.map(_.signature).mkString(","))
}

/**
 * Tuple is a short-hand data structures, which can be used without assigning names to a record and its parameters.
 * @param params
 */
case class SilkTuple(params: Array[SilkType]) extends SilkValueType {
  def signature = "tuple(%s)".format(params.map(_.signature).mkString(","))
}

/**
 * Remote Silk data imported to Silk data. The schema of the imported data will be resolved later.
 * @param refId
 */
case class SilkImport(refId: String) extends SilkValueType {
  def signature = "import(%s)".format(refId)
}


object SilkPackage {

  val root : SilkPackage = SilkRootPackage

  def apply(cl:Class[_], className:String) : SilkPackage = {
    val clName = cl.getName
    val pos = clName.lastIndexOf(className)
    if(pos != -1) {
      apply(clName.substring(0, math.max(0, pos-1)))
    }
    else {
      apply(cl.getPackage)
    }
  }
  
  def apply(packageRef:Package) : SilkPackage =
   apply(packageRef.getName)

  def apply(fullModuleName: String): SilkPackage = {
    val component = fullModuleName.split("\\.")
    if (component.length == 0)
      root
    else {
      new SilkPackage(component)
    }
  }

  private val componentNamePattern = """[A-Za-z][A-Za-z0-9]*""".r
  private[model] def isValidComponentName(name: String) = componentNamePattern.findFirstIn(name).isDefined
}

/**
 * Module for enclosing record definitions
 * @param component
 */
case class SilkPackage(component: Array[String]) extends SilkType {
  for(each <- component; if !SilkPackage.isValidComponentName(each))
    throw new IllegalArmentException("invalid package component name %s in %s".format(each, component.mkString(".")))

  def isRoot = component.isEmpty
  def signature = "package(%s)".format(name)

  def name: String = component.mkString(".")
  def leafName : String = component.last

  override def hashCode = name.hashCode()
  override def equals(obj: Any) : Boolean =
    obj.isInstanceOf[SilkPackage] && (obj.asInstanceOf[SilkPackage].name == name)

}

object SilkRootPackage extends SilkPackage(Array.empty) {
  override def name : String = leafName
  override def leafName : String = "_root"
}

/**
 * Schema definition of Silk data. SilkSchema can be nested
 * @param module
 * @param element
 */
case class SilkSchema(module: SilkPackage, element: Array[SilkSchemaElement]) extends SilkSchemaElement {
  def signature = "schema(%s,[%s])".format(module.signature, element.map(_.signature).mkString(","))
}


case class SilkContextType(name:String, context:SilkValueType)

