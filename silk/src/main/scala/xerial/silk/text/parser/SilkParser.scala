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



object SilkParser {

  sealed abstract class ParseError extends Exception
  case class SyntaxError(posInLine:Int, message:String) extends ParseError
  case object NoMatch extends ParseError
  abstract class Parser {
    def LA1 : Token
    def consume : Unit
  }
  
  abstract class Expr[A] {
    def ~(next: Expr[A]) : Expr[A] = ExprSeq(this, next)
    def |(next: Expr[A]) : Expr[A] = OrExpr(this, next)
    def eval(in:Parser) : Either[ParseError, Parser]
  }

  case class ExprSeq[A](first:Expr[A], second:Expr[A]) extends Expr[A] {
    def eval(in:Parser) = first.eval(in).right.flatMap(second.eval(_))
  }
  case class OrExpr[A](a:Expr[A], b:Expr[A]) extends Expr[A] {
    def eval(in:Parser) = {
      val ea = a.eval(in) 
      ea match {
        case Left(NoMatch) => b.eval(in)
        case Left(_) => b.eval(in) match {
          case Left(_) => ea
          case r => r
        }
        case Right(_) => ea
      }
    }
  }
  case class SymbolExpr[A](t:TokenType) extends Expr[A] {
    def eval(in:Parser) = {
      val t = in.LA1
      Either.cond(t.tokenType == t, {in.consume; in}, NoMatch)
    }
  }
  case class RepeatExpr[A](expr:Expr[A], separator:TokenType) extends Expr[A] {
    def eval(in:Parser) = {
      @tailrec def loop(isFirst:Boolean) : Either[ParseError, Parser] = {
        var toContinue = !isFirst

        if(!isFirst) {
          val t = in.LA1
          if(t.tokenType == separator) {
            in.consume
            toContinue = true
          }
            // do nothing
            Right(in)
          }
        }
        else {


        }
      }

      loop(true)
      
      val e = expr.eval(in)
      e match {
        case Left(NoMatch) => Right(in)
        case Left(_) => e
        case Right(_) => 
      }
    }
  }
  case class OptionExpr[A](expr:Expr[A]) extends Expr[A]

  implicit def expr(t:TokenType) : Expr[SilkToken] = new SymbolExpr(t)
  def repeat[A](expr:Expr[A], separator:TokenType) : Expr[A] = RepeatExpr(expr, separator)
  def option[A](expr:Expr[A]) : Expr[A] = OptionExpr(expr)


  def parse(silk:CharSequence) = {
    val t = SilkLexer.tokenStream(silk)
    new SilkParser(t).parse
  }
  

  
//  abstract class Term[+A <: SilkToken] {
//    def ~[A1 <: A](next:Option[A1]) : Term[A1]
//    def map[B](f: A => B) : Option[B]
//    def flatMap[B](f:A => Option[B]) : Option[B]  
//  }
//  
//  class Match[A <: SilkToken](t:A) extends Term[A] {
//    def ~[A1 <: A](next:Option[A1]) : Term[A1] = next map (new Match[A1](_)) getOrElse NoMatch
//    def map[B](f: A => B) : Option[B] = Some(f(t))
//    def flatMap[B](f:A => Option[B]) : Option[B] = f(t)
//  }
//
//  object NoMatch extends Term[Nothing] {
//    def ~[A1 <: Nothing](next: Option[A1]) = NoMatch
//    def map[B](f: Nothing => B) : Option[B] = None
//    def flatMap[B](f:Nothing => Option[B]) : Option[B] = None
//  }
//  
//  implicit def term(t:Option[SilkToken]) : Term = t map (new Match(_)) getOrElse NoMatch
  
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