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
package xerial.silk.parser;

import org.xerial.core.XerialException;
import org.xerial.util.log.Logger;
import xerial.silk.util.io.BufferedScanner;
import xerial.silk.parser.token.SilkToken;
import xerial.silk.util.ArrayDeque;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;


/**
 * Silk Token scanner
 * 
 * @author leo
 * 
 */
public class SilkLexer
{
    public static enum State {
        INIT, HERE_DOC, NODE_NAME, NODE_VALUE, ATTRIBUTE_NAME, ATTRIBUTE_VALUE
    }

    private static Logger         _logger         = Logger.getLogger(SilkLexer.class);

    private int                   PREFETCH_SIZE   = 10;

    private BufferedScanner scanner;
    private State                 state           = State.INIT;
    private ArrayDeque<SilkToken> tokenQueue      = new ArrayDeque<SilkToken>();
    private long                  nProcessedLines = 0;

    public SilkLexer(InputStream in) {
        this.scanner = new BufferedScanner(in);
    }

    public SilkLexer(Reader in) {
        this.scanner = new BufferedScanner(in);
    }

    public void close() throws IOException {
        scanner.close();
    }

    /**
     * Look ahead k tokens. If there is no token at k, return null
     * 
     * @param k
     * @return
     * @throws XerialException
     */
    public SilkToken LA(int k) throws XerialException {
        if (k == 0)
            throw new IllegalArgumentException("k must be larger than 0");
        while (tokenQueue.size() < k && !hasNoMoreLine()) {
            fill(PREFETCH_SIZE);
        }

        if (tokenQueue.size() < k)
            return null;

        return tokenQueue.peekFirst(k - 1);
    }

    private boolean hasNoMoreLine() throws XerialException {
        return scanner.hasReachedEOF();
    }

    /**
     * Read the next token
     * 
     * @return next token or null if no more token is available
     * @throws XerialException
     */
    public SilkToken next() throws XerialException {
        if (!tokenQueue.isEmpty())
            return tokenQueue.pollFirst();

        if (scanner.hasReachedEOF())
            return null;

        fill(PREFETCH_SIZE);
        return next();
    }

    void fill(int prefetch_lines) throws XerialException {

        // TODO line-based error recovery
        try {
            for (int i = 0; i < prefetch_lines && !hasNoMoreLine(); ++i) {
                CharSequence line = scanner.nextLine();
                SilkLineLexer silkLineLexer = new SilkLineLexer(line, state);
                SilkTokensInLine tokensInLine = silkLineLexer.scan();
                nProcessedLines++;
                tokenQueue.addAll(tokensInLine.tokenList);
                state = tokensInLine.nextLineState;
            }
        }
        catch (IOException e) {
            throw XerialException.convert(e);
        }
    }

}
