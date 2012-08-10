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

package xerial.silk.text.parser

import annotation.tailrec


//--------------------------------------
//
// SilkParser.scala
// Since: 2012/08/10 0:04
//
//--------------------------------------

/**
 * Grammar expressions  
 */
object SilkExpr {
  sealed abstract class ParseError extends Exception
  case class SyntaxError(posInLine:Int, message:String) extends ParseError
  case object NoMatch extends ParseError
  abstract class Parser {
    def LA1 : SilkToken
    def consume : Parser
  }

  type ParseResult = Either[ParseError, Parser]

  abstract class Expr[A] {
    def ~(next: Expr[A]) : Expr[A] = ExprSeq(this, next)
    def |(next: Expr[A]) : Expr[A] = OrExpr(this, next)
    def eval(in:Parser) : ParseResult
  }

  case class ExprSeq[A](first:Expr[A], second:Expr[A]) extends Expr[A] {
    def eval(in:Parser) = first.eval(in).right.flatMap(second.eval(_))
  }
  case class OrExpr[A](a:Expr[A], b:Expr[A]) extends Expr[A] {
    def eval(in:Parser) = {
      val ea = a.eval(in)
      ea match {
        case r @ Right(_) => r
        case Left(NoMatch) => b.eval(in)
        case Left(_) => b.eval(in) match {
          case noMatch @ Left(NoMatch) => noMatch
          case other => other
        }
      }
    }
  }
  case class SymbolExpr[A](t:TokenType) extends Expr[A] {
    def eval(in:Parser) = {
      val t = in.LA1
      Either.cond(t.tokenType == t, in.consume, NoMatch)
    }
  }

  case class ZeroOrMore[A](expr:Expr[A]) extends Expr[A] {
    def eval(in:Parser) = {
      @tailrec def loop(p:Parser) : ParseResult = {
        expr.eval(p) match {
          case Left(NoMatch) => Right(p)
          case l @ Left(_) => l
          case Right(next) => loop(next)
        }
      }
      loop(in)
    }
  }

  case class OneOrMore[A](expr:Expr[A]) extends Expr[A] {
    def eval(in:Parser) = {
      @tailrec def loop(i:Int, p:Parser) : ParseResult = {
        expr.eval(p) match {
          case Left(NoMatch) if i > 0 => Right(p)
          case l @ Left(_) => l
          case Right(next) => loop(i+1, next)
        }
      }
      loop(0, in)
    }
  }

  /**
   * (expr (sep expr)*)?
   * @param expr
   * @param separator
   * @tparam A
   */
  case class RepeatExpr[A](expr:Expr[A], separator:SymbolExpr[A]) extends Expr[A] {
    private val r = option(expr ~ ZeroOrMore(separator ~ expr))
    def eval(in:Parser) = r.eval(in)
  }
  case class OptionExpr[A](expr:Expr[A]) extends Expr[A] {
    def eval(in:Parser) = {
      expr.eval(in) match {
        case l @ Left(NoMatch) => Right(in)
        case other => other
      }
    }
  }

  implicit def expr(t:TokenType) : SymbolExpr[SilkToken] = new SymbolExpr(t)
  def repeat[A](expr:Expr[A], separator:TokenType) : Expr[A] = RepeatExpr(expr, new SymbolExpr[A](separator))
  def oneOrMore[A](expr:Expr[A], separator:TokenType) : Expr[A] = expr ~ ZeroOrMore(new SymbolExpr[A](separator) ~ expr)
  def option[A](expr:Expr[A]) : Expr[A] = OptionExpr(expr)
  
  
}

object SilkParser {

  import Token._
  import SilkExpr._

  private class Parser(token:TokenStream) extends SilkExpr.Parser {
    def LA1 = token.LA(1)
    def consume = { token.consume; this }
  }

  def parse(expr:SilkExpr.Expr[SilkToken], silk:CharSequence) {
    val t = SilkLexer.tokenStream(silk)
    expr.eval(new Parser(t))
  }

  def parse(silk:CharSequence) = {
    val t = SilkLexer.tokenStream(silk)
    new SilkParser(t).parse
  }
  
  
  val silk = DataLine | node | preamble | LineComment | BlankLine
  val node = option(Indent) ~ Name ~ option(nodeParams) ~ option(nodeParamSugar | nodeParams)
  val nodeParamSugar = Separator ~ repeat (param, Comma)
  val nodeParams = LParen ~ repeat(param, Comma) ~ RParen ~ option(Colon ~ NodeValue)
  val param = Name ~ option(Colon ~ value)
  val value = Token.String | Integer | Real | QName | NodeValue | tuple
  val preamble = Preamble ~ QName ~ option(preambleParams)
  val preambleParams = (Separator ~ repeat(preambleParam, Comma)) | (LParen ~ repeat(preambleParam, Comma) ~ RParen) 
  val preambleParam = Name ~ option(Colon ~ preambleParamValue)
  val preambleParamValue = value | typeName
  val typeName = QName ~ option(LSquare ~ oneOrMore(QName, Comma) ~ RSquare)
  val tuple = LParen ~ repeat(value, Comma) ~ RParen
}


/**
 * @author leo
 */
class SilkParser(token:TokenStream) {

  import Token._
  import SilkElement._
  import SilkParser._
  
  private def LA1 = token.LA(1)
  private def LA2 = token.LA(2)
  private def consume = token.consume

  def parseError = throw new SilkParseError 

  def parse : SilkElement = {
    val t = LA1
    t.tokenType match {
      case Token.Preamble =>
      case Indent => node
      case Hyphen => node
      case DataLine =>
    }
  }
  
  def m(expected:TokenType) : SilkToken = {
    val t = LA1
    if(t.tokenType == expected) {
      consume
      t
    }
    else
      parseError
  }

  def t(tokenType:TokenType) : Option[TextToken] = {
    LA1 match {
      case t @ TextToken(_, `tokenType`, text) => consume; Some(t)
      case _ => None
    }
  }

  def text(tokenType:TokenType) : Option[CharSequence] = {
    LA1 match {
      case t @ TextToken(_, `tokenType`, text) => consume; Some(text)
      case _ => None
    }
  }
  def str(tokenType:TokenType) : Option[String] = {
    text(tokenType) map { _.toString }
  }

  
  def p(expected:TokenType) : Option[SilkToken] = {
    val t = LA1
    if(t.tokenType == expected) {
      consume
      Some(t)
    }
    else
      None
  }

//  def preamble : SilkElement = {
//    // preamble := % QName preamble_params?
//    p(Token.Preamble) ~ str(QName) map { name =>
//      SilkElement.Preamble(name, )
//    }
//
//  }
//
//  def preambleParams : Seq[PreambleParam] = {
//    def param : Option[PreambleParam] = {
//      p(Name) ~ p(Colon)
//    }
//
//  }
  

  def indent : Int = {
    val t = LA1
    t match {
      case i:IndentToken => consume; i.indentLength
      case _ => 0
    }
  }

  def node : Node = {
    val indentLength = indent
    val name = text(Name)
    val params = nodeParams
    val value = nodeValue
    SilkElement.Node(indentLength, name, params, value)
  }
  
  def nodeParams : Seq[Node] = {
    def nodeParam : Option[Node] = text(Name) map { name =>
      Node(-1, Some(name), Seq.empty, nodeValue)
    }
    LA1.tokenType match {
      case Separator => repeat(nodeParam, Comma)
      case LParen => {
        val s = repeat(nodeParam, Comma)
        m(RParen)
        s
      }
      case _ => Seq.empty
    }
  }

  def repeat[A](matcher: => Option[A], separator:TokenType) : Seq[A] = {
    val b = Seq.newBuilder[A]
    @tailrec def loop {
      for(s <- p(separator); m <- matcher) {
        b += m
        loop
      }
    }
    for(m <- matcher) {
      b += m
      loop
    }
    b.result
  }

  def nodeValue : Option[CharSequence] = {
    // nodeValue := : NodeValue
//    p(Colon) ~ t(NodeValue) map ( _.text)
  }


}