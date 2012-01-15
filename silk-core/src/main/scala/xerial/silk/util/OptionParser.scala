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

import scala.util.matching.Regex
import scala.util.matching.Regex.Match
import collection.mutable.{ArrayBuffer, HashMap}

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

    def valueType: Class[_]
  }

  class FieldOptionSetter(field: Field) extends OptionSetter {
    def set(obj: Any, value: String) = updateField(obj, field, value)

    def valueType: Class[_] = field.getType
  }

  /**
   * command-line option
   */
  sealed abstract class CLOptionItem(setter: OptionSetter) {
    def set(obj: Any, value: String): Unit

    def takesArgument = false

    def takesMultipleArguments = {
      val t = setter.valueType
      isArray(t) || isSeq(t)
    }
  }

  class CLOption(val opt: option, setter: OptionSetter) extends CLOptionItem(setter) {
    def this(opt: option, field: Field) = this (opt, new FieldOptionSetter(field))

    override def takesArgument: Boolean = {
      basicType(setter.valueType) match {
        case BasicType.Boolean => false
        case _ => true
      }
    }

    def set(obj: Any, value: String): Unit = {
      setter.set(obj, value)
    }
  }

  class CLArgument(val arg: argument, setter: OptionSetter) extends CLOptionItem(setter) {
    def this(arg: argument, field: Field) = this (arg, new FieldOptionSetter(field))

    def set(obj: Any, value: String): Unit = {
      setter.set(obj, value)
    }
  }

  private class OptionTable[A](cl: Class[A]) {
    private def translate[A](f: Field)(implicit m: Manifest[A]): Array[A] = {
      for (a <- f.getDeclaredAnnotations
           if a.annotationType.isAssignableFrom(m.erasure)) yield a.asInstanceOf[A]
    }

    private val options: Array[CLOption] = {
      for (f <- cl.getDeclaredFields; opt <- translate[option](f)) yield new CLOption(opt, f)
    }
    private val args: Array[CLArgument] = {
      val a = for (f <- cl.getDeclaredFields; arg <- translate[argument](f)) yield new CLArgument(arg, f)
      a.sortBy(e => e.arg.index())
    }
    private val symbolTable = {
      val h = new HashMap[String, CLOption]
      options.foreach {
        c =>
          if (!c.opt.symbol.isEmpty)
            h += c.opt.symbol -> c
          if (!c.opt.longName.isEmpty)
            h += c.opt.longName -> c
      }
      h
    }

    def findOption(name: String): Option[CLOption] = symbolTable.get(name)

    def findArgumentItem(argIndex: Int): Option[CLArgument] = {
      if (args.isDefinedAt(argIndex)) Some(args(argIndex)) else None
    }

    def defaultTemplate = """
usage: $COMMAND$ $ARGUMENT_LIST$
	$DESCRIPTION$
[options]
$OPTION_LIST$
"""

    def createUsage(template: String = defaultTemplate): String = {
      val optDscr : Array[(CLOption, String)] = options.map {
        o => {
          val opt : option = o.opt
          val hasShort = opt.symbol.length != 0
          val hasLong = opt.longName.length != 0
          val l = new StringBuilder
          if (hasShort) {
            l append "-%s".format(opt.symbol)
          }
          if (hasLong) {
            if (hasShort)
              l append ", "
            l append "--%s".format(opt.longName)
          }

          if (o.takesArgument) {
            if (hasLong)
              l append ":"
            l append "[%s]".format(opt.varName)
          }
          (o, l.toString)
        }
      }
      val longNameLenMax = options.maxBy(_.opt.longName().length)
      val optDscrLenMax = optDscr.map(_._2.length).max

      val s = optDscr.map{x =>
        val paddingLen = optDscrLenMax - x._2.length
        val padding = Array.fill(paddingLen)(" ").mkString
        " %s%s  %s".format(x._2, padding, x._1.opt.description())
      }.mkString("\n")


      defaultTemplate.replaceAll("""\$OPTION_LIST\$""", s)
    }

  }


  def parse[T](optionClass: Class[T], args: Array[String])(implicit m: Manifest[T]): T = {
    val optionTable = new OptionTable(optionClass)

    val optionHolder = optionClass.getConstructor().newInstance()


    val shortOptionSquashed = """^-([^-\s]\w+)""".r
    val shortOption = """^-(\w)([:=](\w+))?""".r
    val longOption = """^--(\w+)([:=](\w+))?""".r

    def findMatch[A](p: Regex, s: String)(f: Match => Option[A]): Option[A] = {
      p.findFirstMatchIn(s) match {
        case None => None
        case Some(m) => f(m)
      }
    }
    def group(m: Match, group: Int): Option[String] = {
      if (m.start(group) != -1) Some(m.group(group)) else None
    }

    object ShortOption {
      def unapply(s: String): Option[(String, Option[String])] =
        findMatch(shortOption, s)(m => Some((m.group(1), group(m, 3))))
    }

    object ShortOptionSquashed {
      def unapply(s: String): Option[String] = findMatch(shortOptionSquashed, s)(m => Some(m.group(1)))
    }

    object LongOption {
      def unapply(s: String): Option[(String, Option[String])] =
        findMatch(longOption, s)(m => Some((m.group(1), group(m, 3))))
    }


    def traverseArg(l: List[String]): Unit = {
      def setOption(name: String, arg: Option[String], rest: List[String]): List[String] = {
        optionTable.findOption(name) match {
          case None => throw new IllegalArgumentException("Unknown option: %s" format name)
          case Some(opt) => {
            if (opt.takesArgument) {
              arg match {
                case Some(x) => opt.set(optionHolder, x); rest
                case None =>
                  if (rest.length > 0)
                    opt.set(optionHolder, rest(0))
                  else
                    throw new IllegalArgumentException("Option %s needs an argument" format name)
                  rest.tail
              }
            }
            else {
              opt.set(optionHolder, "true")
              rest
            }
          }
        }
      }

      var argIndex = 0
      val unusedArg = new ArrayBuffer[String]()

      def setArgument(arg: String): Unit = {
        optionTable.findArgumentItem(argIndex) match {
          case Some(ai) => {
            ai.set(optionHolder, arg)
            if (!ai.takesMultipleArguments)
              argIndex += 1
          }
          case None => unusedArg += arg
        }
      }

      // Process command line arguments
      var remaining = l
      while (!remaining.isEmpty) {
        remaining = remaining match {
          case ShortOptionSquashed(ops) :: rest => ops.foreach(ch => setOption(ch.toString, None, rest)); rest
          case ShortOption(op, arg) :: rest => setOption(op, arg, rest)
          case LongOption(op, arg) :: rest => setOption(op, arg, rest)
          case e :: rest => setArgument(e); rest
          case Nil => List()
        }
      }
    }

    traverseArg(args.toList)
    optionHolder.asInstanceOf[T]
  }

  def displayHelpMessage[T](cl: Class[T]) = {
    val optionTable = new OptionTable(cl)
    print(optionTable.createUsage())
  }

}

/**
 * @author leo
 */
class OptionParser {


}