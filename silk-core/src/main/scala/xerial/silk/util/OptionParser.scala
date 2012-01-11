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

package xerial.silk
package util

import java.lang.annotation.Annotation
import java.io.File
import java.util.Date
import java.text.DateFormat

//--------------------------------------
//
// OptionParser.scala
// Since: 2012/01/10 13:43
//
//--------------------------------------


object OptionParser extends Logging {

  import java.lang.reflect.{Field, Method}

  private trait OptionSetter {
    def set(obj:Any, value:String) : Unit
  }




  private class FieldOptionSetter(f:Field) extends OptionSetter {
    def set(obj:Any, value:String) : Unit = {
      val accessible = f.isAccessible
      try {
        if(!accessible)
          f.setAccessible(true)

        val converted = TypeUtil.convert(value, ClassManifest.fromClass(f.getType))
        converted match {
          case Some(x) => f.set(obj, converted)
          case None => // do nothing
        }
      }
      finally {
        if(!accessible)
          f.setAccessible(false)
      }

    }
  }


  private class OptionItem(opt: option, setter:OptionSetter)

  //private class FieldOption(name:String, field:Field) extends OptionItem(name)
  
  //private class GetterSetterOption(name:String, getter:Method, setter:Method) extends OptionItem(name)
  
  private class OptionBuilder(val option:List[OptionItem]) {

    def this(cl:Class[_]) {

//      def toOptionItem(f:Field) = {
//        getOption(f.getDeclaredAnnotations) match {
//          case None => None
//          case Some(x) => {
//            case o:option => new FieldOption()
//          }
//        }
//
//      }
//
//      def getOption(x: Array[Annotation]): Option[Annotation] = {
//        x.find { e =>
//          val t = e.annotationType
//          t.isAssignableFrom(classOf[option]) ||
//            t.isAssignableFrom(classOf[argument])
//        }
//      }
      

      this(List.empty[OptionItem])
    }

  }



  def parse[T](optionClass: Class[T], args: Array[String])(implicit m: Manifest[T]): T = {
    val opt = optionClass.getConstructor().newInstance()

    debug {
      "declared methods"
    }
    debug {
      val decl = optionClass.getDeclaredMethods
      decl.mkString("\n")
    }

    def printAnnotation(a: Any) {
      a match {
        case opt: option => debug(opt)
        case arg: argument => debug(arg)
        case _ => // ignore
      }
    }

    def containsOption(x: Array[Annotation]): Boolean = {
      val opt = x.find { e =>
        val t = e.annotationType
        t.isAssignableFrom(classOf[option]) ||
          t.isAssignableFrom(classOf[argument])
      }
      opt.isDefined
    }


    def findOptionGetter = {
      optionClass.getDeclaredMethods.filter {
        m =>
          m.getParameterTypes.size == 0 &&
            containsOption(m.getDeclaredAnnotations) &&
            !(List("toString", "hashCode").contains(m.getName))
      }
    }


    def findOptionField = {
      optionClass.getDeclaredFields.filter {
        x => containsOption(x.getDeclaredAnnotations)
      }
    }

    val getter = findOptionGetter
    debug {
      "getter:\n" + getter.mkString("\n")
    }

    debug {
      "field:\n" + findOptionField.mkString("\n")
    }


    opt
  }

}

/**
 * @author leo
 */
class OptionParser {


}