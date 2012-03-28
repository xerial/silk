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

import collection.mutable
import mutable.ArrayBuffer
import xerial.silk.lens.ObjectSchema.{GenericType, Method}
import xerial.silk.util.TypeUtil._

//--------------------------------------
//
// MethodCallBuilder.scala
// Since: 2012/03/27 16:43
//
//--------------------------------------

/**
 * Builds method call arguments
 * @author leo
 */
class MethodCallBuilder(m:Method, owner:AnyRef) extends GenericBuilder with Logger {
  assert(m.owner == owner.getClass)

  private val valueHolder = mutable.Map[String, Any]()

  // Set the default value of the method
  for(p <- m.params; v <- findDefaultValue(p.name))
    valueHolder += p.name -> v
  
  
  private def findDefaultValue(name:String) : Option[Any] = {
    m.params.find(name == _.name).flatMap{ p =>
      try {
        val methodName = "%s$default$%d".format(m.name, p.index+1)
        val dm = owner.getClass.getMethod(methodName)
        Some(dm.invoke(owner))
      }
      catch {
        case _ => None
      }
    }
  }

  def set(name:String, value:Any) : Unit = {
    m.params.find(name == _.name).foreach{ p =>
      import TypeUtil._
      if(canBuildFromBuffer(p.valueType.rawType)) {
        val t = p.valueType.asInstanceOf[GenericType]
        val gt = t.genericTypes(0).rawType
        type E = gt.type
        val arr = valueHolder.getOrElseUpdate(name, new ArrayBuffer[E]).asInstanceOf[ArrayBuffer[Any]]
        arr += convert(value, gt)
      }
      else {
        valueHolder(name) = convert(value, p.valueType)
      }
    }

  }

  def execute : Any = {
    val args = for(p <- m.params) yield {
      val v = valueHolder.getOrElse(p.name, TypeUtil.zero(p.valueType.rawType))
      convert(v, p.valueType).asInstanceOf[AnyRef]
    }

    trace { "args: " + args.mkString(", ") }

    if(args.isEmpty)
      m.jMethod.invoke(owner)
    else
      m.jMethod.invoke(owner, args:_*)
  }

}

