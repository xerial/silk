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
import xerial.core.log.Logging


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

  sealed abstract class Expr[A] extends Logging {
    trace("Define %s: %s", this.getClass.getSimpleName, toString)

    def ~(next: Expr[A]) : Expr[A] = ExprSeq(this, next)
    def |(next: Expr[A]) : Expr[A] = OrExpr(this, next)
    def eval(in:Parser) : ParseResult
  }

  case class ExprSeq[A](first:Expr[A], second:Expr[A]) extends Expr[A] {
    require(first != null, this.toString)
    require(second != null, this.toString)
    def eval(in:Parser) = first.eval(in).right.flatMap(second.eval(_))
  }
  case class OrExpr[A](a:Expr[A], b:Expr[A]) extends Expr[A] {
    require(a != null, this.toString)
    require(b != null, this.toString)
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
    require(expr != null, toString)
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
    SilkParser.silk.eval(new Parser(t))
  }

  val silk : Expr[SilkToken] = DataLine | LineComment | BlankLine //| preamble
  val nodeParams = LParen ~ repeat(param, Comma) ~ RParen ~ option(Colon ~ NodeValue)
  val nodeParamSugar = Separator ~ repeat(param, Comma)
  val node = option(Indent) ~ Name ~ option(nodeParams) ~ option(nodeParamSugar | nodeParams)
  val preamble = Preamble ~ QName ~ option(preambleParams)
  val preambleParams = (Separator ~ repeat(preambleParam, Comma)) | (LParen ~ repeat(preambleParam, Comma) ~ RParen) 
  val preambleParam = Name ~ option(Colon ~ preambleParamValue)
  val preambleParamValue = value | typeName
  val typeName = QName ~ option(LSquare ~ oneOrMore(QName, Comma) ~ RSquare)

  //val tuple = LParen ~ repeat(value, Comma) ~ RParen
  val value : Expr[SilkToken] = Token.String | Integer | Real | QName | NodeValue //| tuple
  val param = Name ~ option(Colon ~ value)


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




}