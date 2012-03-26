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

import xerial.silk.lens.ObjectSchema
import xerial.silk.lens.ObjectSchema._
import collection.mutable.{ArrayBuffer, ArrayBuilder, Buffer, Map}

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
object ObjectBuilder extends Logger {

  // class ValObj(val p1, val p2, ...)
  // class VarObj(var p1, var p2, ...)

  def apply[A](cl: Class[A]): ObjectBuilder[A] = {

    if (!TypeUtil.canInstantiate(cl))
      throw new IllegalArgumentException("Cannot instantiate class " + cl)

    // collect default values of the object
    val schema = ObjectSchema(cl)
    val prop = Map.newBuilder[String, Any]

    // get the default values (including constructor parameters and fields)
    val default = TypeUtil.newInstance(cl)
    for (p <- schema.parameters) {
      prop += p.name -> p.get(default)
    }

    new ObjectBuilderFromString(cl, prop.result)
  }

}

/**
 * Generic object builder
 * @author leo
 */
trait ObjectBuilder[A] {

  def get(name: String): Option[_]
  def set(name: String, value: Any): Unit

  def build: A
}

class ObjectBuilderFromString[A](cl: Class[A], defaultValue: Map[String, Any]) extends ObjectBuilder[A] with Logger {
  private val schema = ObjectSchema(cl)
  private val valueHolder = collection.mutable.Map[String, Any]()

  import TypeUtil._

  defaultValue.foreach {
    case (name, value) => {
      val v =
        schema.findParameter(name) match {
          case Some(x) =>
            if (canBuildFromBuffer(x.valueType.rawType)) {
              debug("name:%s valueType:%s", name, x.valueType)
              toBuffer(value, x.valueType)
            }
            else
              value
          case None => value
        }
      valueHolder += name -> v
    }
  }

  def get(name: String) = valueHolder.get(name)

  def set(name: String, value: Any) {
    val p = schema.getParameter(name)
    updateValueHolder(name, p.valueType, value)
  }

  private def updateValueHolder(name: String, valueType: ValueType, value: Any): Unit = {
    if (canBuildFromBuffer(valueType.rawType)) {
      debug("update value holder name:%s, valueType:%s with value:%s", name, valueType, value)
      val t = valueType.asInstanceOf[GenericType]
      val gt = t.genericTypes(0).rawType
      type E = gt.type
      val arr = valueHolder.getOrElseUpdate(name, new ArrayBuffer[E]).asInstanceOf[ArrayBuffer[Any]]
      arr += convert(value, gt)
    }
    else {
      valueHolder(name) = value
    }
  }

  def build: A = {

    val cc = schema.constructor

    var remainingParams = schema.parameters.map(_.name).toSet

    def getValue(p: Parameter) = convert(valueHolder.getOrElse(p.name, TypeUtil.zero(p.valueType.rawType)), p.valueType)

    // Prepare constructor args
    val args = for (p <- cc.params) yield {
      val v = getValue(p)
      remainingParams -= p.name
      v.asInstanceOf[AnyRef]
    }

    debug("cc:%s, args:%s", cc, args.mkString(", "))
    val res = cc.newInstance(args).asInstanceOf[A]

    // Set the remaining parameters
    debug("remaining params: %s", remainingParams.mkString(", "))
    for (pname <- remainingParams) {
      schema.getParameter(pname) match {
        case f@FieldParameter(owner, name, valueType) =>
          TypeUtil.updateField(res, f.field, getValue(f))
        case _ => // ignore constructor/method parameters
      }
    }

    res
  }
}