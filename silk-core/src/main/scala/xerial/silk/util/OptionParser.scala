package xerial.silk
package util

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

import scala.util.matching.Regex
import scala.util.matching.Regex.Match
import collection.mutable.ArrayBuffer
import scala.util.parsing.combinator.RegexParsers
import lens.ObjectSchema
import lens.ObjectSchema._

//--------------------------------------
//
// OptionParser.scala
// Since: 2012/01/10 13:43
//
//--------------------------------------

/**
 * Tokenize single string representations of command line arguments into Array[String]
 */
object CommandLineTokenizer extends RegexParsers with Logger {

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

/**
 * Creates option parsers
 */
object OptionParser extends Logger {

  import java.lang.reflect.Field
  import TypeUtil._

  def tokenize(line: String): Array[String] = CommandLineTokenizer.tokenize(line)

  def apply[A <: AnyRef](cl: Class[A]): OptionParser[A] = {
    newOptionParser(cl)
  }

  def apply[A <: AnyRef](optionHolder: A): OptionParser[A] = {
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

  protected trait OptionSetter {
    def set(obj: AnyRef, value: String): Unit

    def get(obj: AnyRef): Any

    val valueType: Class[_]

    val valueName: String
  }

  protected class FieldOptionSetter(field: Field) extends OptionSetter {
    def set(obj: AnyRef, value: String) = updateField(obj, field, value)

    def get(obj: AnyRef) = readField(obj, field)

    val valueType: Class[_] = field.getType

    val valueName = field.getName
  }

  /**
   * command-line option
   */
  protected sealed abstract class CLOptionItem(val param: Parameter) {

    def takesArgument = false

    def takesMultipleArguments = {
      val t = param.valueType.rawType
      isArray(t) || isSeq(t)
    }
  }

  protected class CLOption(val annot: option, param: Parameter) extends CLOptionItem(param) {
    override def takesArgument: Boolean = !param.valueType.isBooleanType
  }

  protected class CLArgument(val arg: argument, param:Parameter) extends CLOptionItem(param) {
    def name: String = {
      var n = arg.name
      if (n.isEmpty)
        n = param.name
      n
    }
  }

  /**
   * Schema of the command line options
   * @param cl
   */
  protected class OptionSchema(cl: Class[_]) {

    private val schema = ObjectSchema(cl)

    private[OptionParser] val options: Array[CLOption] = {
      for (p <- schema.parameters; opt <- p.findAnnotationOf[option])
      yield new CLOption(opt, p)
    }

    private[OptionParser] val args: Array[CLArgument] = {
      val argParams = for (p <- schema.parameters; arg <- p.findAnnotationOf[argument])
      yield new CLArgument(arg, p)
      argParams.sortBy(x => x.arg.index())
    }

    private val symbolTable = {
      var h = Map[String, CLOption]()
      options.foreach {
        case opt:CLOption =>
          if (!opt.annot.symbol.isEmpty)
            h += opt.annot.symbol -> opt
          if (!opt.annot.longName.isEmpty)
            h += opt.annot.longName -> opt
      }
      h
    }

    def apply(name: String): CLOption = symbolTable.apply(name)

    def findOption(name: String): Option[CLOption] = symbolTable.get(name)

    def findArgumentItem(argIndex: Int): Option[CLArgument] = {
      if (args.isDefinedAt(argIndex)) Some(args(argIndex)) else None
    }
  }

  private def newOptionParser[A <: AnyRef](optionClass: Class[A]) = {
    def newOptionHolder = {
      if (TypeUtil.hasDefaultConstructor(optionClass))
        optionClass.getConstructor().newInstance().asInstanceOf[A]
      else {
        // Create proxy
        //createOptionHolderProxy(optionClass)
        null.asInstanceOf[A]
      }
    }
    new OptionParser(newOptionHolder)
  }

  val defaultUsageTemplate = """usage: $COMMAND$ $ARGUMENT_LIST$
$DESCRIPTION$
[options]
$OPTION_LIST$
"""

}

/**
 * @author leo
 */
class OptionParser[A <: AnyRef](val optionHolder: A) {

  import OptionParser._

  private val optionTable = new OptionSchema(optionHolder.getClass)

  /**
   * Parse and fill the option holder
   * @param args
   * @param exitAfterFirstArgument
   * @return unused arguments
   */
  def parse(args: Array[String], exitAfterFirstArgument: Boolean = false, ignoreUnknownOption: Boolean = false): Array[String] = {

    def findMatch[T](p: Regex, s: String)(f: Match => Option[T]): Option[T] = {
      p.findFirstMatchIn(s) match {
        case None => None
        case Some(m) => f(m)
      }
    }

    def group(m: Match, group: Int): Option[String] = {
      if (m.start(group) != -1) Some(m.group(group)) else None
    }

    object ShortOrLongOption {
      private val pattern = """^-{1,2}(\w)""".r

      def unapply(s: List[String]): Option[(CLOption, List[String])] = {
        findMatch(pattern, s.head) {
          m =>
            val symbol = m.group(1)
            optionTable.findOption(symbol) match {
              case None => None
              case Some(opt) => {
                if (opt.takesArgument)
                  None
                else
                  Some((opt, s.tail))
              }
            }
        }
      }
    }

    object ShortOrLongOptionWithArgument {
      private val pattern = """^-{1,2}(\w)([:=](\w+))?""".r

      def unapply(s: List[String]): Option[(CLOption, String, List[String])] = {
        findMatch(pattern, s.head) {
          m =>
            val symbol = m.group(1)
            val immediateArg = group(m, 3)
            optionTable.findOption(symbol) match {
              case None => None
              case Some(opt) => {
                if (!opt.takesArgument)
                  None
                else {
                  if (immediateArg.isEmpty) {
                    if (s.tail.isEmpty)
                      throw new IllegalArgumentException("Option %s needs an argument" format opt)
                    else {
                      val remaining = s.tail
                      Some((opt, remaining.head, remaining.tail))
                    }
                  }
                  else
                    Some((opt, immediateArg.get, s.tail))
                }
              }
            }
        }
      }
    }

    object ShortOptionSquashed {
      private val pattern = """^-([^-\s]\w+)""".r

      def unapply(s: List[String]): Option[(List[CLOption], List[String])] = {
        findMatch(pattern, s.head) {
          m =>
            val squashedOptionSymbols = m.group(1)
            val (known, unknown) = squashedOptionSymbols.partition(ch => isKnownOption(ch.toString))
            if (!unknown.isEmpty)
              throw new IllegalArgumentException("unknown option is squashed: " + s.head)
            Some((known.map(ch => optionTable(ch.toString)).toList, s.tail))
        }
      }
    }

    def isKnownOption(name: String): Boolean = optionTable.findOption(name).isDefined

    def traverseArg(l: List[String]): Array[String] = {

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
      var unused = Array.empty
      while (continue && !remaining.isEmpty) {
        val next = remaining match {
          case ShortOptionSquashed(ops, rest) => {
            ops.foreach(opt => opt.set(optionHolder, "true"))
            (rest, unused)
          }
          case ShortOrLongOption(op, rest) => {
            op.set(optionHolder, "true")
            (rest, unused)
          }
          case ShortOrLongOptionWithArgument(op, arg, rest) => {
            op.set(optionHolder, arg)
            (rest, unused)
          }
          case e :: rest => {
            setArgument(e);
            if (exitAfterFirstArgument)
              continue = false
            (rest, unused)
          }
          case Nil => (List(), unused)
        }
        remaining = next._1
        unused = next._2
      }
      remaining.toArray
    }

    traverseArg(args.toList)
  }

  def printUsage = {
    print(createUsage())
  }

  def createOptionHelpMessage = {
    val optDscr: Array[(CLOption, String)] = for (o <- optionTable.options)
    yield {
      val opt: option = o.annot
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

    def genDescription(opt: CLOption) = {
      if (opt.takesArgument) {
        "%s (default:%s)".format(opt.annot.description(), opt.get(optionHolder))
      }
      else
        opt.annot.description()
    }

    val s = for (x <- optDscr) yield {
      val paddingLen = optDscrLenMax - x._2.length
      val padding = Array.fill(paddingLen)(" ").mkString
      " %s%s  %s".format(x._2, padding, genDescription(x._1))
    }
    s.mkString("\n") + "\n"
  }

  private def createArgList = {
    val l = for (a <- optionTable.args) yield {
      a.name
    }
    l.map("[%s]".format(_)).mkString(" ")
  }

  def createUsage(template: String = defaultUsageTemplate): String = {
    StringTemplate.eval(template) {
      Map('ARGUMENT_LIST -> createArgList, 'OPTION_LIST -> createOptionHelpMessage)
    }

  }

}


