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

  def parserOf[A](implicit m:ClassManifest[A]) : OptionParser = {
    val cl = m.erasure
    val schema = new ClassOptionSchema(cl)
    new OptionParser(schema)
  }

  def newParser[A](optionHolder:A) = {
    val cl = optionHolder.getClass
    new OptionParser(new ClassOptionSchema(cl))
  }

  def parse[A <: AnyRef](args: Array[String])(implicit m:ClassManifest[A]): A = {
    val parser = parserOf[A]
    parser.parse(args)
    // TODO
    null.asInstanceOf[A]
  }

  def parse[A <: AnyRef](argLine: String)(implicit m:ClassManifest[A]): A = {
    parse(tokenize(argLine))
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

//  private def newOptionParser[A <: AnyRef](optionClass: Class[A]) = {
//    def newOptionHolder = {
//      if (TypeUtil.hasDefaultConstructor(optionClass))
//        optionClass.getConstructor().newInstance().asInstanceOf[A]
//      else {
//        // Create proxy
//        //createOptionHolderProxy(optionClass)
//        null.asInstanceOf[A]
//      }
//    }
//    new OptionParser(newOptionHolder)
//  }

  val defaultUsageTemplate = """usage: $COMMAND$ $ARGUMENT_LIST$
$DESCRIPTION$
[options]
$OPTION_LIST$
"""

}

/**
 * command-line option
 */
sealed abstract class CLOptionItem(val param: Parameter) {

  def takesArgument: Boolean = false

  def takesMultipleArguments: Boolean = {
    val t: Class[_] = param.valueType.rawType
    TypeUtil.isArray(t) || TypeUtil.isSeq(t)
  }
}

/**
 * Command line option and the associated class parameter
 * @param annot
 * @param param
 */
class CLOption(val annot: option, param: Parameter) extends CLOptionItem(param) {
  override def takesArgument: Boolean = !param.valueType.isBooleanType
}

/**
 * Command line argument type and the associated class parameter
 * @param arg
 * @param param
 */
class CLArgument(val arg: argument, param: Parameter) extends CLOptionItem(param) {
  def name: String = {
    var n = arg.name
    if (n.isEmpty)
      n = param.name
    n
  }
}



/**
 * Schema of the command line options
 */
trait OptionSchema {

  def options : Array[CLOption]
  def args : Array[CLArgument] // must be sorted by arg.index in ascending order

  protected val symbolTable = {
    var h = Map[String, CLOption]()
    options.foreach {
      case opt: CLOption =>
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

class ClassOptionSchema(cl:Class[_]) extends OptionSchema {
  private val schema = ObjectSchema(cl)

  val options: Array[CLOption] = {
    for (p <- schema.parameters; opt <- p.findAnnotationOf[option])
    yield new CLOption(opt, p)
  }

  val args: Array[CLArgument] = {
    val argParams = for (p <- schema.parameters; arg <- p.findAnnotationOf[argument])
    yield new CLArgument(arg, p)
    argParams.sortBy(x => x.arg.index())
  }

}

class MethodOptionSchema(method:Method) extends OptionSchema {

  val options = 
    for(p<-method.params; opt<-p.findAnnotationOf[option]) yield new CLOption(opt, p)

  val args = {
    val l = for(p<-method.params; arg<-p.findAnnotationOf[argument]) yield new CLArgument(arg, p)
    l.sortBy(x => x.arg.index())
  }

}



/**
 * Option -> value mapping result
 */
sealed abstract class OptionMapping
case class OptSetFlag(opt:CLOption)  extends OptionMapping
case class OptMapping(opt:CLOption, value:String) extends OptionMapping
case class OptMappingMultiple(opt:CLOption, value:Array[String]) extends OptionMapping
case class ArgMapping(opt:CLArgument, value:String) extends OptionMapping
case class ArgMappingMultiple(opt:CLArgument, value:Array[String]) extends OptionMapping


class OptionParserResult(val mapping: Seq[OptionMapping], val unusedArgument: Array[String]) {
}


/**
 * Command-line argument parser
 *
 * @author leo
 */
class OptionParser(val optionTable: OptionSchema) {

  import OptionParser._

//  def build[A](args:Array[String])(implicit m:ClassManifest[A]) : A {
//    val result = parse(args)
//    val b = ObjectBuilder(m.erasure)
//    for(each <- result) {
//      case OptSetFlag(opt) =>
//        opt.param match {
//          case ConstructorParameter(owner, name, index, valueType) =>
//          case FieldParameter(owner, name, valueType) => TypeUtil.set
//          case MethodParameter(owner, name, index, valueType) =>
//        }
//    }
//  }


  /**
   * Parse the command-line argumetns
   * @param args
   * @param exitAfterFirstArgument
   * @return parse result
   */
  def parse(args: Array[String], exitAfterFirstArgument: Boolean = false): OptionParserResult = {

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

    // Hold mapping, option -> args ... 
    val optionValues = collection.mutable.Map[CLOptionItem, ArrayBuffer[String]]()
    val unusedArguments = new ArrayBuffer[String]

    def traverseArg(l: List[String]): Unit = {
      var argIndex = 0

      def appendOptionValue(ci: CLOptionItem, value: String): Unit = {
        val holder = optionValues.getOrElseUpdate(ci, new ArrayBuffer[String]())
        holder += value
      }

      def setArgument(arg: String): Unit = {
        optionTable.findArgumentItem(argIndex) match {
          case Some(ai) => {
            appendOptionValue(ai, arg)
            if (!ai.takesMultipleArguments)
              argIndex += 1
          }
          case None => unusedArguments += arg
        }
      }

      // Process command line arguments
      var continue = true
      var remaining = l
      while (continue && !remaining.isEmpty) {
        val next = remaining match {
          case ShortOptionSquashed(ops, rest) => {
            ops.foreach(opt => appendOptionValue(opt, "true"))
            rest
          }
          case ShortOrLongOption(op, rest) => {
            appendOptionValue(op, "true")
            rest
          }
          case ShortOrLongOptionWithArgument(op, arg, rest) => {
            appendOptionValue(op, arg)
            rest
          }
          case e :: rest => {
            setArgument(e)
            if (exitAfterFirstArgument)
              continue = false
            rest
          }
          case Nil => List()
        }
        remaining = next
      }
    }

    traverseArg(args.toList)

    val mapping : Seq[OptionMapping] = {
      val m : TraversableOnce[OptionMapping] = for ((opt, values) <- optionValues) yield {
        opt match {
          case c:CLOption =>
            if(c.takesArgument) {
              if(c.takesMultipleArguments)
                OptMappingMultiple(c, values.toArray)
              else
                OptMapping(c, values(0))
            }
            else
              OptSetFlag(c)
          case a:CLArgument =>
            if(a.takesMultipleArguments)
              ArgMappingMultiple(a, values.toArray)
            else
              ArgMapping(a, values(0))
        }
      }
      m.toSeq
    }
    new OptionParserResult(mapping, unusedArguments.toArray)
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
//      if (opt.takesArgument) {
//        "%s (default:%s)".format(opt.annot.description(),
//      }
//      else
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

