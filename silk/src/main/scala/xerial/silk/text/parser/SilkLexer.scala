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

import token.SilkToken
import xerial.core.log.Logging
import xerial.core.io.text.LineReader
import java.io.{Reader, InputStream}
import xerial.silk.util.ArrayDeque


object SilkLexer {
  object INIT extends SilkLexerState
  object HERE_DOC extends SilkLexerState
  object NODE_NAME extends SilkLexerState
  object NODE_VALUE extends SilkLexerState
  object ATTRIBUTE_NAME extends SilkLexerState
  object ATTRIBUTE_VALUE extends SilkLexerState
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
  private var state = INIT
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
    for(i <- 0 until prefetch_lines if noMoreLine) {
      val line = reader.nextLine
      val lexer = new SilkLineLexer(line, state)
      val tokensInLine = lexer.scan
      nProcessedLines += 1
      tokenQueue.addAll(tokensInLine.tokenList)
      state = tokensInLine.nextLineState
    }
  }

}
