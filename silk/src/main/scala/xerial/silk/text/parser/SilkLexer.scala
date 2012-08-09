/*--------------------------------------------------------------------------
 *  Copyright 2011 Taro L. Saito
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *--------------------------------------------------------------------------*/
//--------------------------------------
// XerialJ
//
// SilkTokenScanner.java
// Since: 2011/04/29 22:53:43
//
// $URL$
// $Author$
//--------------------------------------
package xerial.silk.text.parser

import xerial.core.log.Logging
import xerial.core.io.text.LineReader
import java.io.{Reader, InputStream}
import xerial.silk.util.ArrayDeque
import annotation.tailrec


object SilkLexer {
  object INIT extends SilkLexerState
  object HERE_DOC extends SilkLexerState
  object NODE_NAME extends SilkLexerState
  object NODE_VALUE extends SilkLexerState
  object ATTRIBUTE_NAME extends SilkLexerState
  object ATTRIBUTE_VALUE extends SilkLexerState

  def parseLine(silk:CharSequence) : Seq[SilkToken] = {
    val (tokens, nextState) = new SilkLineLexer(silk, INIT).scan
    tokens
  }
}


sealed abstract class SilkLexerState() {
  override def toString = this.getClass.getSimpleName.replaceAll("\\$", "")
}



/**
 * Silk Token scanner
 * 
 * @author leo
 * 
 */
class SilkLexer(reader:LineReader) extends Logging {
  import xerial.silk.text.parser.SilkLexer._

  def this(in:InputStream) = this(LineReader(in))
  def this(in:Reader) = this(LineReader(in))

  private val PREFETCH_SIZE   = 10
  private var state : SilkLexerState = INIT
  private var nProcessedLines = 0L
  private val tokenQueue = new ArrayDeque[SilkToken]

  def close = reader.close

  /**
   * Look ahead k tokens. If there is no token at k, return null
   *
   * @param k
   * @return
   * @throws XerialException
   */
  def LA(k:Int) : SilkToken = {
    if (k == 0)
      throw new IllegalArgumentException("k must be larger than 0");
    while (tokenQueue.size() < k && !noMoreLine) {
      fill(PREFETCH_SIZE);
    }

    if (tokenQueue.size() < k)
      null
    else
      tokenQueue.peekFirst(k - 1)
  }

  private def noMoreLine : Boolean = reader.reachedEOF

    /**
     * Read the next token
     * 
     * @return next token or null if no more token is available
     * @throws XerialException
     */
    def next : SilkToken = {
      if (!tokenQueue.isEmpty())
        tokenQueue.pollFirst()
      else if (noMoreLine)
        null
      else {
        fill(PREFETCH_SIZE);
        next
      }
    }

  def fill(prefetch_lines:Int) {
    // TODO line-based error recovery
    for(i <- 0 until prefetch_lines) {
      for(line <- reader.nextLine) {
        val lexer = new SilkLineLexer(line, state)
        val (tokens, nextState) = lexer.scan
        nProcessedLines += 1
        tokens foreach (tokenQueue.add(_))
        state = nextState
      }
    }
  }

}


case class SilkParseError() extends Exception

/**
 * @author leo
 */
class SilkLineLexer(line: CharSequence, initialState: SilkLexerState) extends Logging {

  import SilkLexer._

  private val scanner = LineReader(line)
  private var posInLine: Int = 0
  private var state = initialState
  private var nextLineState: SilkLexerState = INIT
  private val tokenQueue = Seq.newBuilder[SilkToken]

  private def consume {
    scanner.consume
    posInLine += 1
  }
  
  private def emit(token: SilkToken): Unit = tokenQueue += token
  private def emit(t: TokenType) : Unit = emit(Token(scanner.markStart, t))
  private def emit(tokenChar: Int) : Unit = emit(Token.toSymbol(tokenChar))
  private def emitWithText(t: TokenType) : Unit = emitWithText(t, scanner.selected)
  private def emitWithText(t: TokenType, text: CharSequence) : Unit = emit(TextToken(scanner.markStart, t, text))
  private def emitTrimmed(t: TokenType) : Unit = emitWithText(t, scanner.trimSelected)
  private def emitWholeLine(t: TokenType) : Unit = emitWithText(t, scanner.selectedFromFirstMark)

  def scan : (Seq[SilkToken], SilkLexerState) = {
    while (!scanner.reachedEOF) {
      scanner.resetMarks
      scanner.mark
      state match {
        case INIT => mInit
        case NODE_NAME => mToken
        case ATTRIBUTE_NAME => mToken
        case ATTRIBUTE_VALUE => mToken
        case NODE_VALUE => mNodeValue
        case HERE_DOC => mHereDoc
      }
    }
    
    (tokenQueue.result(),  nextLineState)
  }

  private def LA1 = scanner.LA(1)

  def mIndent: Int = {
    @tailrec def loop(len: Int): Int = LA1 match {
      case ' ' => {
        consume;
        loop(len + 1)
      }
      case '\t' => {
        consume;
        loop(len + 4)
      } // TAB is 4 white spaces
      case _ => len
    }
    loop(0)
  }

  def matchUntilEOL {
    while ( {
      val c = LA1;
      c != LineReader.EOF
    })
      consume
  }

  def mWhiteSpace_s {
    while ( {
      val c = LA1;
      c == ' ' || c == '\t'
    }) // (' ' | '\t') *
      consume
  }

  def mEscapeSequence {
    m('\\')
    LA1 match {
      case '"' => consume
      case '\\' => consume
      case '/' => consume
      case 'b' => consume
      case 'f' => consume
      case 'n' => consume
      case 'r' => consume
      case 't' => consume
      case 'u' => for (i <- 0 until 4) mHexDigit
      case _ => error
    }
  }

  def isDigit(ch: Int) = ch >= '0' && ch <= '9'

  def mHexDigit {
    val c = LA1
    if (isDigit(c) || c >= 'A' && c <= 'F' || c >= 'a' && c >= 'f')
      consume
    else
      error
  }

  def mDigit: Boolean = if (isDigit(LA1)) {
    consume; true
  } else false

  def mDigit_s {
    while (mDigit) {}
  }

  def mDigit_p {
    if (mDigit) mDigit_s else error
  }

  def mExp: Boolean = {
    val c = LA1
    if (c == 'e' || c == 'E') {
      consume
      val c2 = LA1
      if (c2 == '+' || c2 == '-') consume
      mDigit_p
      true
    }
    else
      false
  }

  def mNumber {
    var c = LA1
    if (c == '-') {
      // negative number
      val c2 = scanner.LA(2)
      if (isDigit(c2)) {
        consume; c = c2
      }
    }

    if (c == '0') consume
    else if (c >= '1' && c <= '9') {
      consume; mDigit_s
    }

    LA1 match {
      case '.' => consume; mDigit_p; mExp; emitWithText(Token.Real)
      case _ => if (mExp) emitWithText(Token.Real) else emitWithText(Token.Integer)
    }
  }

  def mString {
    m('"')
    @tailrec def loop {
      LA1 match {
        case '"' => consume
        case '\\' => mEscapeSequence; loop
        case LineReader.EOF => error
        case _ => consume; loop
      }
    }
    loop
  }

  def error: Nothing = throw new SilkParseError()

  def m(expected: Int) {
    val c = scanner.LA(1)
    if (c != expected)
      throw error
    else
      consume
  }

  def skipWhiteSpaces {
    mWhiteSpace_s; scanner.mark
  }


  def mInit: Unit = LA1 match {
    case ' ' => emit(IndentToken(posInLine, mIndent))
    case '\t' => emit(IndentToken(posInLine, mIndent))
    case '-' =>
      if (isDigit(scanner.LA(2))) {
        matchUntilEOL
        emitWithText(Token.DataLine)
      }
      else {
        consume
        LA1 match {
          case '-' => consume; emit(Token.HereDocSep); nextLineState = HERE_DOC
          case _ => emit(Token.Hyphen)
        }
        state = NODE_NAME
      }
//    case '>' => consume; emit(Token.SeqNode); state = NODE_NAME
    case '#' => consume; matchUntilEOL; emitWithText(Token.LineComment)
    case '%' => consume; emit(Token.Preamble); state = NODE_NAME
    case '@' => consume; emit(Token.At); state = NODE_NAME
    case LineReader.EOF => emit(Token.BlankLine)
    case '\\' =>
      val c2 = scanner.LA(2)
      if (c2 == '-') {
        consume;
        scanner.mark
      } // escaped '-'
      matchUntilEOL
      emitWithText(Token.DataLine)
    case _ =>
      matchUntilEOL
      emitWithText(Token.DataLine)
  }

  def mToken: Unit = {

    def transitCh(ch: Int, nextState: SilkLexerState): Unit = transit(Token.toSymbol(ch), nextState)
    def transit(t: TokenSymbol, nextState: SilkLexerState): Unit = {
      consume; emit(t); state = nextState
    }
    def noTransition(ch: Int): Unit = {
      consume; emit(ch)
    }

    skipWhiteSpaces

    val c = LA1
    c match {
      case '(' => transitCh(c, ATTRIBUTE_NAME)
      case ')' => transitCh(c, ATTRIBUTE_NAME)
      case '-' =>
        consume
        state match {
          case NODE_NAME => transit(Token.Separator, ATTRIBUTE_NAME)
          case ATTRIBUTE_NAME => transit(Token.Separator, ATTRIBUTE_NAME)
          case _ =>
            val c2 = LA1
            if (isDigit(c2)) // is number?
              mNumber
            else
              emitTrimmed(Token.NodeValue)
        }
      case ':' =>
        state match {
          case NODE_NAME => transit(Token.Colon, NODE_VALUE)
          case ATTRIBUTE_NAME => transit(Token.Colon, ATTRIBUTE_VALUE)
          case _ => error
        }
      case ',' =>
        state match {
          case ATTRIBUTE_VALUE => transit(Token.Comma, ATTRIBUTE_NAME)
          case _ => transit(Token.Comma, state)
        }
      case '@' => noTransition(c)
      case '<' => noTransition(c)
      case '>' => noTransition(c)
      case '[' => noTransition(c)
      case '?' => noTransition(c)
      case '*' => noTransition(c)
      case '"' => mString
      case '+' =>
        consume
        state match {
          case ATTRIBUTE_VALUE => emitTrimmed(Token.NodeValue)
          case _ => emit(Token.Plus)
        }
      case LineReader.EOF =>
      case _ =>
        if (isDigit(c))
          mNumber
        else {
          mQName
          emitTrimmed(Token.QName)
        }
    }
  }

  def mQName {
    // qname first:  Alphabet | Dot | '_' | At | Sharp
    val c = LA1
    if (c == '@' || c == '#' || c == '.' || c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'))
      consume
    else
      error

    while ( {
      val c = LA1;
      (c == '.' || c == '_' || c == ' ' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || isDigit(c))
    })
      consume
  }


  def mNodeValue {
    skipWhiteSpaces
    matchUntilEOL
    emitTrimmed(Token.NodeValue)
  }

  def mHereDoc {
    mWhiteSpace_s

    var toContinue = true
    if (LA1 == '-') {
      consume
      if (LA1 == '-') {
        consume;
        matchUntilEOL;
        emit(Token.HereDocSep)
        state = INIT
        nextLineState = INIT
        toContinue = false
      }
    }

    if (toContinue) {
      matchUntilEOL; emitWholeLine(Token.HereDoc)
    }
  }


}