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

//--------------------------------------
//
// SilkToken.scala
// Since: 2012/08/03 18:57
//
//--------------------------------------

object Token {
  object EOF extends TokenType
  object Indent extends TokenType
  object NewLine extends TokenType
  object QName extends TokenType
  object String extends TokenType

  object Integer extends TokenType
  object Real extends TokenType
  object DataLine extends TokenType
  object HereDoc extends TokenType
  object BlankLine extends TokenType
  object NodeValue extends TokenType

  object Hyphen extends TokenSymbol("-")
  object HereDocSep extends TokenSymbol("--")
  object LineComment extends TokenSymbol("#")

  object Preamble extends TokenSymbol("%")
  object At extends TokenSymbol("@")
  object Colon extends TokenSymbol(":")
  object Comma extends TokenSymbol(",")
  object Asterisk extends TokenSymbol("*")
  object Plus extends TokenSymbol("+")
  object Question extends TokenSymbol("?")
  object LParen extends TokenSymbol("(")
  object RParen extends TokenSymbol(")")
  object LBracket extends TokenSymbol("[")
  object RBracket extends TokenSymbol("]")
  object LSquare extends TokenSymbol("<")
  object RSquare extends TokenSymbol(">")

  def toVisibleString(s: CharSequence): String = {
    if (s == null) return ""
    var text: String = s.toString
    text = text.replaceAll("\n", "\\\\n")
    text = text.replaceAll("\r", "\\\\r")
    text = text.replaceAll("\t", "\\\\t")
    return text
  }
}

abstract class TokenType {
  val name = this.getClass.getSimpleName.replaceAll("\\$", "")
  override def toString = name
}

sealed abstract class TokenSymbol(val symbol:String) extends TokenType

sealed abstract class SilkToken(posInLine:Int, tokenType:TokenType) {
  override def toString: String = "pos:%2d [%s]" format(posInLine, tokenType)
}

case class Token(override val posInLine:Int, override val tokenType:TokenType) extends SilkToken(posInLine, tokenType)

case class TextToken(override val posInLine:Int, override val tokenType:TokenType, text:String) extends SilkToken(posInLine, tokenType) {
  override def toString: String = "pos:%2d [%s] %s" format(posInLine, tokenType, Token.toVisibleString(text))
}


