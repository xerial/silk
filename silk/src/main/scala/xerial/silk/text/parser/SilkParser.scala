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
  case class SyntaxError(posInLine: Int, message: String) extends ParseError
  case object NoMatch extends ParseError
  abstract class Parser {
    def LA1: SilkToken
    def consume: Parser
  }

  type ParseResult = Either[ParseError, Parser]


//  class TreeRegistry[A] extends Logging {
//    private val table = collection.mutable.Map[String, Tree]()
//
//    def apply(e: Expr[A]): Tree = {
//      debug("search %s", e.name)
//      table.getOrElseUpdate(e.name, e.mkTree(this))
//    }
//  }

  sealed abstract class Expr[A] extends Logging {
    self: Expr[A] =>

    def name: String = hashCode.toString

//    def mkTree(r: TreeRegistry[A]): Tree
//    def tree(r: TreeRegistry[A]): Tree = r(self)

    def ~(next: => Expr[A]): Expr[A] = new Expr[A] {
//      def mkTree(r: TreeRegistry[A]) = SeqNode(r(self), r(next))
      def eval(in: Parser): ParseResult = {
        //trace("eval ~ - first:%s, second:%s : LA1 = %s", self, next, in.LA1)
        self.eval(in).right.flatMap(next.eval(_))
      }
    }

    def |(next: => Expr[A]): Expr[A] = new Expr[A] {
//      def mkTree(r: TreeRegistry[A]) = OrNode(r(self), r(next))
      def eval(in: Parser): ParseResult = {
        val ea = self.eval(in)
        ea match {
          case r@Right(_) => r
          case Left(NoMatch) => next.eval(in)
          case Left(_) => next.eval(in) match {
            case noMatch@Left(NoMatch) => noMatch
            case other => other
          }
        }
      }
    }
    def eval(in: Parser): ParseResult
  }

  case class Single[A](tt: TokenType) extends Expr[A] {
    trace("Define single token expr: %s", tt)
    override def name = tt.name
//    def mkTree(r: TreeRegistry[A]) = Leaf(tt)
    def eval(in: Parser): ParseResult = {
      val t = in.LA1
      trace("eval %s, LA1:%s", t.tokenType, t)
      if (t.tokenType == tt) {
        debug("match %s, LA1:%s", t.tokenType, t)
        Right(in.consume)
      }
      else
        Left(NoMatch)
    }
  }

  implicit def expr(t: TokenType): Single[SilkToken] = new Single(t)
  /**
   * (expr (sep expr)*)?
   */
  def repeat[A](expr: => Expr[A], separator: TokenType): Expr[A] = new Expr[A] {
    private val p = option(expr ~ zeroOrMore(new Single[A](separator) ~ expr))
//    def mkTree(r: TreeRegistry[A]) = p.mkTree(r)
    def eval(in: Parser) = p.eval(in)
  }

  def zeroOrMore[A](expr: => Expr[A]) = new Expr[A] {
//    def mkTree(r: TreeRegistry[A]) = ZeroOrMore(r(expr))
    def eval(in: Parser) = {
      @tailrec def loop(p: Parser): ParseResult = {
        expr.eval(p) match {
          case Left(NoMatch) => Right(p)
          case l@Left(_) => l
          case Right(next) => loop(next)
        }
      }
      loop(in)
    }
  }

  def oneOrMore[A](expr: => Expr[A]) = new Expr[A] {
//    def mkTree(r: TreeRegistry[A]) = OneOrMore(r(expr))
    def eval(in: Parser) = {
      @tailrec def loop(i: Int, p: Parser): ParseResult = {
        expr.eval(p) match {
          case Left(NoMatch) if i > 0 => Right(p)
          case l@Left(_) => l
          case Right(next) => loop(i + 1, next)
        }
      }
      loop(0, in)
    }
  }

  def oneOrMore[A](expr: => Expr[A], separator: TokenType): Expr[A] = new Expr[A] {
    val p = expr ~ zeroOrMore(new Single[A](separator) ~ expr)
//    def mkTree(r: TreeRegistry[A]) = p.mkTree(r)
    def eval(in: Parser) = p.eval(in)
  }

  def option[A](expr: => Expr[A]): Expr[A] = new Expr[A] {
//    def mkTree(r: TreeRegistry[A]) = OptionNode(r(expr))
    def eval(in: Parser) = {
      expr.eval(in) match {
        case l@Left(NoMatch) => Right(in)
        case other => other
      }
    }
  }

  class ExprRef(name: String) extends Expr[SilkToken] {
//    def mkTree(r: TreeRegistry[SilkToken]) = null
    def eval(in: Parser) = null
  }

}

trait Grammar extends Logging {

  abstract class Parser {
    def LA1: SilkToken
    def consume: Parser
  }

  abstract class Tree(val name:String) { a : Tree =>
    def ~(b: Tree) : Tree = SeqNode(Seq(a, b))
    def |(b: Tree) : Tree = OrNode(Seq(a, b))
    //def eval(in:Parser) : ParseResult
    override def toString = name
  }

  case class TreeRef(override val name:String) extends Tree(name) {
  }

  case class Leaf(tt: TokenType) extends Tree(tt.name) {

  }


  case class OrNode(seq:Seq[Tree]) extends Tree(seq.map(_.name).mkString(" | ")) {
    override def |(b: Tree) : Tree = OrNode(seq :+ b)
  }

  case class SeqNode(seq:Seq[Tree]) extends Tree(seq.map(_.name).mkString(" ")) {
    override def ~(b: Tree) : Tree = SeqNode(seq :+ b)
  }

  case class OneOrMore(a: Tree) extends Tree("(%s)+".format(a.name))
  case class ZeroOrMore(a: Tree) extends Tree("(%s)*".format(a.name))
  case class OptionNode(a: Tree) extends Tree("(%s)?".format(a.name))


  case class Repeat(a:Tree, separator:TokenType) extends Tree("rep(%s,%s)".format(a.name, separator.name))



  def findTreeName : String = {
    val exclude = Seq("findTreeName", "repeat", "oneOrMore", "option")
    new Exception().getStackTrace.view.map(_.getMethodName).find(n => exclude.forall(n != _)) getOrElse("unknown")
  }
  def repeat(expr: Tree, separator: TokenType): Tree = Repeat(expr, separator)
  def oneOrMore(expr: Tree, separator: TokenType) : Tree = (expr ~ ZeroOrMore(Leaf(separator) ~ expr))
  def option(expr: Tree): Tree = OptionNode(expr)


  protected implicit def toLeaf(tt:TokenType) : Tree = Leaf(tt)

  protected implicit def toRule(s:String) = new Rule(s)
  protected implicit def toRef(s:String) = new TreeRef(s)

  class Rule(name:String) {
    def :=(t:Tree) : TreeRef = {
      debug("new rule %s := %s", name, t)
      ruleTable += name -> t
      TreeRef(name)
    }
  }

  private val ruleTable = collection.mutable.Map[String, Tree]()
  def getRule(name:String) = ruleTable.get(name)

}


object SilkParser extends Grammar with Logging {

  private class Parser(token: TokenStream) extends SilkExpr.Parser {
    def LA1 = token.LA(1)
    def consume = {
      token.consume
      this
    }
  }

  def parse(rule:String, silk:String) = {
    getRule(rule)
  }


  import Token._

  // Silk grammar rules
  "expr" := String | LParen ~ "expr" ~ RParen
  "silk" := DataLine | "node" | "preamble" | LineComment | BlankLine
  "preamble" := Preamble ~ QName ~ option("preambleParams")
  "preambleParams" := (Separator ~ repeat("preambleParam", Comma)) | (LParen ~ repeat("preambleParam", Comma) ~ RParen)
  "preambleParam" := Name ~ option(Colon ~ "preambleParamValue")
  "preambleParamValue" := "value" | "typeName"
  "typeName" := QName ~ option(LSquare ~ oneOrMore(QName, Comma) ~ RSquare)
  "node" := option(Indent) ~ Name ~ option("nodeParams") ~ option("nodeParamSugar" | "nodeParams")
  "nodeParamSugar" := Separator ~ repeat("param", Comma)
  "nodeParams" := LParen ~ repeat("param", Comma) ~ RParen ~ option(Colon ~ NodeValue)
  "param" := Name ~ option(Colon ~ "value")
  "value" := NodeValue | QName | Token.String | Integer | Real | "tuple"
  "tuple" := LParen ~ repeat("value", Comma) ~ RParen
}


/**
 * @author leo
 */
class SilkParser(token: TokenStream) {

  import Token._
  import SilkElement._
  import SilkParser._

  private def LA1 = token.LA(1)
  private def LA2 = token.LA(2)
  private def consume = token.consume


}