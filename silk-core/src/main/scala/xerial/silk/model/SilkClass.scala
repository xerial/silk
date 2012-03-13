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

import xerial.silk.util.{Logging, Cache}


//--------------------------------------
//
// SilkClass.scala
// Since: 2012/01/24 13:37
//
//--------------------------------------

object SilkPackage {

  val packageCache = Cache[String, SilkPackage](newPackage)

  private def newPackage(packageName: String) = {
    val c: Array[String] = if (packageName.isEmpty) Array.empty else packageName.split("\\.")
    new SilkPackage(c)
  }

  def apply(packageName: String): SilkPackage = packageCache(packageName)

  val root = SilkPackage("")
}

/**
 * package name
 */
class SilkPackage private[model](component: Array[String]) extends Logging {
  for (each <- component; if !SilkClass.isValidComponentName(each)) {
    throw new IllegalArgumentException("invalid component name %s in %s".format(each, component.mkString(".")))
  }

  val name = if (component.isEmpty) "_root" else component.mkString(".")

  override def toString = name
}

object SilkClass extends Logging {

  private def newClass(fullClassName: String, attribute: Array[SilkAttribute]) = {
    trace {
      "Create a new class " + fullClassName
    }
    val p = fullClassName.split("\\.")

    p.length match {
      case 1 => new SilkClass(SilkPackage.root, p(0), attribute)
      case i if i >= 2 => new SilkClass(SilkPackage(p.init.mkString(".")), p.last, attribute)
      case _ => throw new IllegalArgumentException("invalid class name %s".format(fullClassName))
    }
  }

  def apply(fullClassName: String, attribute: Array[SilkAttribute] = Array.empty): SilkClass = newClass(fullClassName, attribute)



}

/**
 * class definition of silk
 * @author leo
 */
class SilkClass private[model](val silkPackage: SilkPackage, val name: String, val attribute: Array[SilkAttribute]) {
  if (!SilkClass.isValidComponentName(name)) throw new IllegalArgumentException("invalid class name " + name)

  def fullName = "%s.%s".format(silkPackage, name)

  override def toString = fullName

}


class SilkAttribute(val name: String, val typeName: SilkValueType) {

}

