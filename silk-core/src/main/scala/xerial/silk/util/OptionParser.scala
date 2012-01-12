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

import collection.mutable.HashMap

//--------------------------------------
//
// OptionParser.scala
// Since: 2012/01/10 13:43
//
//--------------------------------------


object OptionParser extends Logging {

  import java.lang.reflect.{Field, Method}
  import TypeUtil._

  trait OptionSetter {
    def set(obj: Any, value: String): Unit
  }

  class FieldOptionSetter(field:Field) extends OptionSetter {
    def set(obj: Any, value:String) = updateField(obj, field, value)
  }

  sealed trait CLOptionItem  {

  }

  class CLOption(val opt: option, val setter: OptionSetter) extends CLOptionItem {
    def this(opt:option, field:Field) = this(opt, new FieldOptionSetter(field))
  }
  
  class CLArgument(val arg:argument, val setter:OptionSetter) extends CLOptionItem {
    def this(arg:argument,  field:Field) = this(arg, new FieldOptionSetter(field))
  }

  private class OptionTable[A](cl:Class[A]) {
    private val table : Array[CLOptionItem] = findOptionItem(cl)
    private val symbolTable = new HashMap[String, CLOptionItem]
    private val longNameTable = new HashMap[String, CLOptionItem]

    {
      table.foreach {
        case c:CLOption => {
          if(!c.opt.symbol.isEmpty)
            symbolTable += c.opt.symbol -> c
          if(!c.opt.longName.isEmpty)
            longNameTable += c.opt.longName -> c
        }
        case _ =>
      }
    }
    
    def containsSymbol(s:String) = symbolTable.contains(s)
    def containsLongName(s:String) = longNameTable.contains(s)

    def findOptionItem[A](cl: Class[A]): Array[CLOptionItem] = {

      def translate[A](f:Field)(implicit m:Manifest[A]) : Array[A] = {
        for(a <- f.getDeclaredAnnotations
            if a.annotationType.isAssignableFrom(m.erasure)
        ) yield { a.asInstanceOf[A] }
      }

      def getOptionAnnotation(f: Field) = translate[option](f)
      def getArgumentAnnotation(f:Field) = {
        for(a <- f.getDeclaredAnnotations
            if a.annotationType.isAssignableFrom(classOf[argument]))
        yield a.asInstanceOf[argument]
      }

      val fields = cl.getDeclaredFields
      val o = for(f <- fields; opt <- getOptionAnnotation(f))
      yield { new CLOption(opt, f)}
      val a = for(f <- fields; arg <- getArgumentAnnotation(f))
      yield { new CLArgument(arg, f)}

      o ++ a
    }
  }


  def parse[T](optionClass: Class[T], args: Array[String])(implicit m: Manifest[T]): T = {
    val optionTable = new OptionTable(optionClass)
    
    def traverseArg(l:List[String]) : Unit = {
      l match {
        case flag :: rest if(flag.matches("^-[~-]")) => {
          // short option (-)
          flag.substring(1).foreach { ch => 
            
          }
        }
        case flag :: rest if(flag.matches("^--")) => {
          // long option (--)
        
        }
        case e :: rest => {
          // add argument

          traverseArg(rest)
        }
        case Nil =>
      }
    }

    val argl = args.toList
    traverseArg(argl)

    val opt = optionClass.getConstructor().newInstance()
    opt
  }

}

/**
 * @author leo
 */
class OptionParser {


}