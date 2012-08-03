//--------------------------------------
//
// SilkLineLexer.scala
// Since: 2012/08/03 3:53 PM
//
//--------------------------------------

package xerial.silk.text.parser

import token.{SilkTextToken, SilkIndentToken, SilkToken, SilkTokenType}
import xerial.core.io.text.LineReader
import collection.mutable
import annotation.tailrec


case class SilkParseError extends Exception

/**
 * @author leo
 */
class SilkLineLexer(line: CharSequence, initialState: SilkLexerState) {

  import SilkLexer._

  private val scanner = LineReader(line)
  private var posInLine: Int = 0
  private var state = initialState
  private var nextLineState: SilkLexerState = INIT
  private var tokenQueue = mutable.Queue[SilkToken]()

  private def noMoreToken = scanner.reachedEOF

  private def consume {
    scanner.consume; posInLine += 1
  }

  private def emit(token: SilkToken) : Unit = tokenQueue += token
  private def emit(t: SilkTokenType) = emit(new SilkToken(t, posInLine))
  private def emit(tokenChar: Int) = emit(SilkTokenType.toTokenType(tokenChar))
  private def emitWithText(t: SilkTokenType) = emitWithText(t, scanner.selected)
  private def emitWithText(t: SilkTokenType, text: CharSequence) =  emit(new SilkTextToken(t, text, posInLine))
  private def emitTrimmed(t:SilkTokenType) = emitWithText(t, scanner.trimSelected)
  private def emitWholeLine(t:SilkTokenType) = emitWithText(t, scanner.selectedFromFirstMark)

  def scan {
    def loop {
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
          case _ => // parse error
        }
      }
    }

    loop
  }

  private def LA1 = scanner.LA(1)

  def mIndent: Int = {
    @tailrec def loop(len: Int): Int = LA1 match {
      case ' ' => {
        consume; loop(len + 1)
      }
      case '\t' => {
        consume; loop(len + 4)
      } // TAB is 4 white spaces
      case _ => len
    }
    loop(0)
  }

  def matchUntilEOL {
    while ( {
      val c = LA1; c != LineReader.EOF
    })
      consume
  }

  def mWhiteSpace_s {
    while ( {
      val c = LA1; c == ' ' || c == '\t'
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

  def isDigit(ch:Int) = ch >= '0' && ch <= '9'

  def mHexDigit {
    val c = LA1
    if (isDigit(c) || c >= 'A' && c <= 'F' || c >= 'a' && c >= 'f')
      consume
    else
      error
  }

  def mDigit: Boolean = if (isDigit(LA1)) { consume; true } else false

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
    if(c == '-') { // negative number
      val c2 = scanner.LA(2)
      if(isDigit(c2)) { consume; c = c2 }
    }

    if(c == '0') consume
    else if(c >= '1' && c <= '9') { consume; mDigit_s }

    LA1 match {
      case '.' => consume; mDigit_p; mExp; emitWithText(SilkTokenType.Real)
      case _ => if(mExp) emitWithText(SilkTokenType.Real) else emitWithText(SilkTokenType.Integer)
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

  def skipWhiteSpaces { mWhiteSpace_s; scanner.mark }




  def mInit: Unit = LA1 match {
    case ' ' => emit(new SilkIndentToken(mIndent))
    case '\t' => emit(new SilkIndentToken(mIndent))
    case '-' =>
      if (isDigit(scanner.LA(2))){
        matchUntilEOL
        emitWithText(SilkTokenType.DataLine)
      }
      else {
        consume
        LA1 match {
          case '-' => consume; emit(SilkTokenType.HereDocSep); nextLineState = HERE_DOC
          case '*' => consume; emit(SilkTokenType.BlockNode)
          case '|' => consume; emit(SilkTokenType.TabNode)
          case _ => emit(SilkTokenType.Node)
        }
        state = NODE_NAME
      }
    case '>' => consume; emit(SilkTokenType.SeqNode); state = NODE_NAME
    case '#' => consume; matchUntilEOL; emitWithText(SilkTokenType.LineComment)
    case '%' => consume; emit(SilkTokenType.Preamble); state = NODE_NAME
    case '@' => consume; emit(SilkTokenType.At); state = NODE_NAME
    case LineReader.EOF => emit(SilkTokenType.BlankLine)
    case '\\' =>
      val c2 = scanner.LA(2)
      if (c2 == '-') {
        consume; scanner.mark
      } // escaped '-'
      matchUntilEOL
      emitWithText(SilkTokenType.DataLine)
    case _ =>
      matchUntilEOL
      emitWithText(SilkTokenType.DataLine)
  }

  def mToken : Unit = {

    def transitCh(ch:Int, nextState:SilkLexerState) : Unit =  transit(SilkTokenType.toTokenType(ch), nextState)
    def transit(t:SilkTokenType, nextState:SilkLexerState) : Unit =  { consume; emit(t); state=nextState }
    def notransit(ch:Int) : Unit = { consume; emit(ch) }

    skipWhiteSpaces

    val c = LA1
    c match {
      case '(' => transitCh(c, ATTRIBUTE_NAME)
      case ')' => transitCh(c, ATTRIBUTE_NAME)
      case '-' =>
        consume
        state match {
          case NODE_NAME => transit(SilkTokenType.Separator, ATTRIBUTE_NAME)
          case ATTRIBUTE_NAME => transit(SilkTokenType.Separator, ATTRIBUTE_NAME)
          case _ =>
            val c2 = LA1
            if(isDigit(c2)) // is number?
              mNumber
            else
              emitTrimmed(SilkTokenType.NodeValue)
        }
      case ':' =>
        state match {
          case NODE_NAME => transit(SilkTokenType.Colon, NODE_VALUE)
          case ATTRIBUTE_NAME => transit(SilkTokenType.Colon, ATTRIBUTE_VALUE)
        }
      case ',' =>
        state match {
          case ATTRIBUTE_VALUE => transit(SilkTokenType.Comma, ATTRIBUTE_NAME)
          case _ => transit(SilkTokenType.Comma, state)
        }
      case '@' => notransit(c)
      case '<' => notransit(c)
      case '>' => notransit(c)
      case '[' => notransit(c)
      case '?' => notransit(c)
      case '*' => notransit(c)
      case '"' => mString
      case '+' =>
        consume
        state match {
          case ATTRIBUTE_VALUE => emitTrimmed(SilkTokenType.NodeValue)
          case _ => emit(SilkTokenType.Plus)
        }
      case LineReader.EOF =>
      case _ => if(isDigit(c)) mNumber else { mQName; emitTrimmed(SilkTokenType.QName) }
    }
  }

  def mQName {
    // qname first:  Alphabet | Dot | '_' | At | Sharp
    val c = LA1
    if (c == '@' || c == '#' || c == '.' || c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z')
      consume
    else
      error

    while({ val c = LA1;
      (c == '.' || c == '_' || c == ' ' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || isDigit(c)) })
      consume
  }


  def mNodeValue {
    skipWhiteSpaces
    matchUntilEOL
    emitTrimmed(SilkTokenType.NodeValue)
  }

  def mHereDoc {
    mWhiteSpace_s

    var toContinue = true
    if(LA1 == '-') {
      consume
      if(LA1 == '-') {
        consume; matchUntilEOL; emit(SilkTokenType.HereDocSep)
        state = INIT
        nextLineState = INIT
        toContinue = false
      }
    }

    if(toContinue) { matchUntilEOL; emitWholeLine(SilkTokenType.HereDoc) }
  }


}