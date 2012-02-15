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
import java.lang.reflect.{Modifier, InvocationHandler}
import javassist.util.proxy.{MethodHandler, MethodFilter, ProxyFactory}
import javassist.{CtNewConstructor, CtConstructor, ClassPool, CtClass}
import scala.util.parsing.combinator.RegexParsers


//--------------------------------------
//
// OptionParser.scala
// Since: 2012/01/10 13:43
//
//--------------------------------------

object CommandLineTokenizer extends RegexParsers with Logging {

  protected def unquote(s: String): String = s.substring(1, s.length() - 1)

  protected def stringLiteral: Parser[String] =
    ("\"" + """([^"\p{Cntrl}\\]|\\[\\/bfnrt]|\\u[a-fA-F0-9]{4})*""" + "\"").r ^^ {
      unquote(_)
    }

  protected def quotation: Parser[String] =
    ("'" + """([^'\p{Cntrl}\\]|\\[\\/bfnrt]|\\u[a-fA-F0-9]{4})*""" + "'").r ^^ {
      unquote(_)
    }

  protected def other: Parser[String] = """([^\"'\s]+)""".r

  protected def token: Parser[String] = stringLiteral | quotation | other

  protected def tokens: Parser[List[String]] = rep(token)

  def tokenize(line: String): Array[String] = {
    val p = parseAll(tokens, line)
    val r = p match {
      case Success(result, next) => result
      case Error(msg, next) => {
        warn {
          msg
        }
        List.empty
      }
      case Failure(msg, next) => {
        warn {
          msg
        }
        List.empty
      }
    }
    r.toArray
  }
}


object OptionParser extends Logging {

  import java.lang.reflect.{Field, Method}
  import TypeUtil._


  def apply[A <: AnyRef](cl: Class[A]): OptionParser[A] = {
    newOptionParser(cl)
  }
  def apply[A <: AnyRef](optionHolder: A) : OptionParser[A] = {
    new OptionParser(optionHolder)
  }


  def parse[A <: AnyRef](cl: Class[A], argLine: String): A = {
    parse(cl, CommandLineTokenizer.tokenize(argLine))
  }

  def parse[A <: AnyRef](cl: Class[A], args: Array[String]): A = {
    val opt = newOptionParser(cl)
    opt.parse(args)
    opt.optionHolder
  }

  def parse[A <: AnyRef](optionHolder: A, args: Array[String]): A = {
    val opt = new OptionParser(optionHolder)
    opt.parse(args)
    opt.optionHolder
  }

  def displayHelpMessage[A](cl: Class[A]): Unit = {
    new OptionParser(cl).displayHelpMessage
  }

  protected trait OptionSetter {
    def set(obj: Any, value: String): Unit

    val valueType: Class[_]

    val valueName: String
  }

  protected class FieldOptionSetter(field: Field) extends OptionSetter {
    def set(obj: Any, value: String) = updateField(obj, field, value)

    val valueType: Class[_] = field.getType

    val valueName = field.getName
  }

  /**
   * command-line option
   */
  protected sealed abstract class CLOptionItem(setter: OptionSetter) {
    def set(obj: Any, value: String): Unit

    def takesArgument = false

    def takesMultipleArguments = {
      val t = setter.valueType
      isArray(t) || isSeq(t)
    }
  }

  protected class CLOption(val opt: option, setter: OptionSetter) extends CLOptionItem(setter) {
    def this(opt: option, field: Field) = this(opt, new FieldOptionSetter(field))

    override def takesArgument: Boolean = {
      basicTypeOf(setter.valueType) match {
        case BasicType.Boolean => false
        case _ => true
      }
    }

    def set(obj: Any, value: String): Unit = {
      setter.set(obj, value)
    }
  }

  protected class CLArgument(val arg: argument, setter: OptionSetter) extends CLOptionItem(setter) {
    def this(arg: argument, field: Field) = this(arg, new FieldOptionSetter(field))

    def name: String = {
      var n = arg.name
      if (n.isEmpty)
        n = setter.valueName
      n
    }


    def set(obj: Any, value: String): Unit = {
      setter.set(obj, value)
    }
  }

  protected class OptionTable(cl: Class[_]) {
    private def translate[T](f: Field)(implicit m: Manifest[T]): Array[T] = {
      for (a <- f.getDeclaredAnnotations
           if a.annotationType.isAssignableFrom(m.erasure)) yield a.asInstanceOf[T]
    }


    private[OptionParser] val options: Array[CLOption] = {
      //cl.getConstructors.flatMap(_.getParameterAnnotations.flatMap(_))

      for (f <- cl.getDeclaredFields; opt <- translate[option](f)) yield new CLOption(opt, f)
    }
    private[OptionParser] val args: Array[CLArgument] = {
      val a = for (f <- cl.getDeclaredFields; arg <- translate[argument](f)) yield new CLArgument(arg, f)
      a.sortBy(e => e.arg.index())
    }
    private val symbolTable = {
      var h = Map[String, CLOption]()
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


  }

  val defaultTemplate = """usage: $COMMAND$ $ARGUMENT_LIST$
$DESCRIPTION$
[options]
$OPTION_LIST$
"""


  private def newOptionParser[A <: AnyRef](optionClass: Class[A], helpTemplate: String = OptionParser.defaultTemplate) = {
    def newOptionHolder = {
      if (TypeUtil.hasDefaultConstructor(optionClass))
        optionClass.getConstructor().newInstance().asInstanceOf[A]
      else {
        // Create proxy
        //createOptionHolderProxy(optionClass)
        null.asInstanceOf[A]
      }
    }
    new OptionParser(newOptionHolder, helpTemplate)
  }


}

/**
 * @author leo
 */
class OptionParser[A <: AnyRef](val optionHolder: A, helpTemplate: String = OptionParser.defaultTemplate) {

  import OptionParser._

  private val optionTable = new OptionTable(optionHolder.getClass)


  def parseUntilFirstArgument(args:Array[String]) : List[String] = {
    parse(args, exitAfterFirstArgument=true)
  }

  /**
   * Parse and fill the option holder
   * @param args
   * @param exitAfterFirstArgument
   * @return unused arguments
   */
  def parse(args: Array[String], exitAfterFirstArgument:Boolean=false): List[String] = {

    def findMatch[T](p: Regex, s: String)(f: Match => Option[T]): Option[T] = {
      p.findFirstMatchIn(s) match {
        case None => None
        case Some(m) => f(m)
      }
    }

    def group(m: Match, group: Int): Option[String] = {
      if (m.start(group) != -1) Some(m.group(group)) else None
    }

    object ShortOption {
      private val shortOption = """^-(\w)([:=](\w+))?""".r

      def unapply(s: String): Option[(String, Option[String])] =
        findMatch(shortOption, s)(m => Some((m.group(1), group(m, 3))))
    }

    object ShortOptionSquashed {
      private val shortOptionSquashed = """^-([^-\s]\w+)""".r

      def unapply(s: String): Option[String] = findMatch(shortOptionSquashed, s)(m => Some(m.group(1)))
    }

    object LongOption {
      private val longOption = """^--(\w+)([:=](\w+))?""".r

      def unapply(s: String): Option[(String, Option[String])] =
        findMatch(longOption, s)(m => Some((m.group(1), group(m, 3))))
    }


    def traverseArg(l: List[String]): List[String] = {
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
      var continue = true
      var remaining = l
      while (continue && !remaining.isEmpty) {
        remaining = remaining match {
          case ShortOptionSquashed(ops) :: rest => ops.foreach(ch => setOption(ch.toString, None, rest)); rest
          case ShortOption(op, arg) :: rest => setOption(op, arg, rest)
          case LongOption(op, arg) :: rest => setOption(op, arg, rest)
          case e :: rest => {
            setArgument(e);
            if(exitAfterFirstArgument)
              continue = false
            rest
          }
          case Nil => List()
        }
      }
      remaining
    }

    traverseArg(args.toList)
  }

  def displayHelpMessage = {
    print(createUsage())
  }


  def createOptionHelpMessage = {
    val optDscr: Array[(CLOption, String)] = for (o <- optionTable.options)
    yield {
      val opt: option = o.opt
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
        else if (hasShort)
          l append " "
        l append "[%s]".format(opt.varName)
      }
      (o, l.toString)
    }

    val optDscrLenMax =
      if (optDscr.isEmpty) 0
      else optDscr.map(_._2.length).max


    val s = for (x <- optDscr) yield {
      val paddingLen = optDscrLenMax - x._2.length
      val padding = Array.fill(paddingLen)(" ").mkString
      " %s%s  %s".format(x._2, padding, x._1.opt.description())
    }
    s.mkString("\n") + "\n"
  }

  private def createArgList = {
    val l = for (a <- optionTable.args) yield {
      a.name
    }
    l.map("[%s]".format(_)).mkString(" ")
  }

  def createUsage(template: String = defaultTemplate): String = {
    StringTemplate.eval(template) {
      Map('ARGUMENT_LIST -> createArgList, 'OPTION_LIST -> createOptionHelpMessage)
    }

  }


}


