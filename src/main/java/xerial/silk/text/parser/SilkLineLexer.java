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
//--------------------------------------
// XerialJ
//
// SilkLineLexer.java
// Since: 2011/06/08 16:12:53
//
// $URL$
// $Author$
//--------------------------------------
package xerial.silk.text.parser;

import org.xerial.core.XerialError;
import org.xerial.core.XerialErrorCode;
import org.xerial.core.XerialException;
import xerial.silk.text.parser.SilkLexer.State;
import xerial.silk.text.parser.token.*;
import xerial.silk.util.io.BufferedScanner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Lexical scanner for reading a single Silk line
 * 
 * @author leo
 * 
 */
public class SilkLineLexer
{
    private int             posInLine     = 0;
    private CharSequence    line;
    private BufferedScanner scanner;
    private State           state         = State.INIT;
    private State           nextLineState = State.INIT;
    private List<SilkToken> tokenQueue    = new ArrayList<SilkToken>();

    public static SilkTokensInLine scan(CharSequence line) throws XerialException {
        SilkLineLexer lexer = new SilkLineLexer(line, State.INIT);
        return lexer.scan();
    }

    public SilkLineLexer(CharSequence line, State initialState) {
        this.line = line;
        this.scanner = new BufferedScanner(line);
        this.state = initialState;
        this.nextLineState = initialState;
    }

    private boolean hasNoMoreToken() {
        return scanner.hasReachedEOF();
    }

    public SilkTokensInLine scan() throws XerialException {
        loop: while (!hasNoMoreToken()) {
            try {
                scanner.resetMarks();
                scanner.mark();
                switch (state) {
                case INIT:
                    mInit();
                    break;
                case NODE_NAME:
                case ATTRIBUTE_NAME:
                case ATTRIBUTE_VALUE:
                    mToken();
                    break;
                case NODE_VALUE:
                    mNodeValue();
                    break;
                case HERE_DOC:
                    mHereDoc();
                    break;
                default:
                    throw new XerialException(XerialErrorCode.PARSE_ERROR, "invalid state: " + state);
                }

            }
            catch (IOException e) {
                throw XerialException.convert(e);
            }
            catch (XerialException e) {
                XerialErrorCode code = e.getErrorCode();
                switch (code) {
                case PARSE_ERROR:
                    // skip this line
                    emit(new SilkErrorToken(posInLine, line, e.getMessage()));
                    break loop;
                case IO_EXCEPTION:
                    // IO-related error cannot be recovered
                    throw e;
                }
            }
        }
        return new SilkTokensInLine(line, tokenQueue, nextLineState);
    }

    private void consume() throws IOException {
        scanner.consume();
        posInLine++;
    }

    private void mHereDoc() throws IOException {
        matchWhiteSpace_s();
        int c = scanner.LA(1);
        if (c == '-') {
            consume();
            int c2 = scanner.LA(1);
            if (c2 == '-') {
                consume();
                matchUntilNewLine();
                emit(SilkTokenType.HereDocSep);
                state = State.INIT;
                nextLineState = State.INIT;
                return;
            }
        }

        matchUntilNewLine();
        emitWholeLine(SilkTokenType.HereDoc);
    }

    private void emit(int tokenChar) {
        SilkTokenType t = SilkTokenType.toTokenType(tokenChar);
        if (t == null)
            throw new XerialError(XerialErrorCode.INVALID_TOKEN, String.format("token %s has no symbol",
                    (char) tokenChar));
        emit(t);
    }

    private void emit(SilkToken token) {
        tokenQueue.add(token);
    }

    private void emit(SilkTokenType type) {
        emit(new SilkToken(type, posInLine));
    }

    private void emitWholeLine(SilkTokenType type) {
        emitWithText(type, scanner.selectedRawStringFromFirstMark());
    }

    private void emitWithText(SilkTokenType type) {
        emitWithText(type, scanner.selectedRawString());
    }

    private void emitWithText(SilkTokenType type, CharSequence text) {
        emit(new SilkTextToken(type, text, posInLine));
    }

    private void emitString() {
        emitWithText(SilkTokenType.String, scanner.selectedRawString(1));
    }

    private void emitTrimmed(SilkTokenType type) {
        emitWithText(type, scanner.selectedRawStringWithTrimming());
    }

    private XerialException error(int expctedChar, int actualChar) {
        return new XerialException(XerialErrorCode.PARSE_ERROR, String.format("Expected:%s, Actual:'%s'. State:%s",
                (char) expctedChar, (char) actualChar, state));
    }

    private XerialException error(String expctedTokenType, int actual) {
        return new XerialException(XerialErrorCode.PARSE_ERROR, String.format(
                "Expected token type:%s, but found '%s'. State:%s", expctedTokenType, (char) actual, state));
    }

    protected void match(int expectedChar) throws XerialException, IOException {
        int c = scanner.LA(1);
        if (c != expectedChar) {
            throw error(expectedChar, c);
        }

        consume();
    }

    private void matchDigit_s() throws IOException {
        for (;;) {
            int c = scanner.LA(1);
            if (c >= '0' && c <= '9')
                consume();
            else
                return;
        }
    }

    private void matchDigit_p() throws IOException, XerialException {
        int c = scanner.LA(1);
        if (c >= '0' && c <= '9')
            consume();
        else
            throw error("Digit+", c);

        matchDigit_s();
    }

    public void matchEscapeSequence() throws IOException, XerialException {
        match('\\');
        int c = scanner.LA(1);
        switch (c) {
        case '"':
        case '\\':
        case '/':
        case 'b':
        case 'f':
        case 'n':
        case 'r':
        case 't':
            consume();
            break;
        case 'u':
            consume();
            for (int i = 0; i < 4; ++i) {
                matchHexDigit();
            }
            break;
        default:
            throw error("escape sequence", c);
        }
    }

    private boolean matchExp() throws XerialException, IOException {
        {
            int c = scanner.LA(1);
            if (c == 'e' || c == 'E')
                consume();
            else
                return false;
        }

        {
            int c = scanner.LA(1);
            if (c == '+' || c == '-') {
                consume();
            }
            matchDigit_p();
        }

        return true;
    }

    public void matchHexDigit() throws XerialException, IOException {
        int c = scanner.LA(1);
        if (c >= '0' && c <= '9' || c >= 'A' && c <= 'F' || c >= 'a' && c >= 'f') {
            consume();
        }
        else {
            throw error("hex digit", c);
        }
    }

    public int matchIndent() throws XerialException, IOException {

        int indentLength = 0;
        for (;;) {
            int c = scanner.LA(1);
            if (c == ' ') {
                indentLength++;
                consume();
            }
            else if (c == '\t') {
                indentLength += 4; // TAB is handled as 4 white spaces
                consume();
            }
            else
                break;
        }
        return indentLength;
    }

    public void matchQName() throws XerialException, IOException {

        {
            // qname first:  Alphabet | Dot | '_' | At | Sharp
            int c = scanner.LA(1);
            if (c == '@' || c == '#' || c == '.' || c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z') {
                consume();
            }
            else {
                throw error("QName", c);
            }
        }

        {
            // qname: <qname first> | ' ' 
            for (;;) {

                int c = scanner.LA(1);
                if (c == '.' || c == '_' || c == ' ' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0'
                        && c <= '9') {
                    consume();
                    continue;
                }
                else
                    return;
            }
        }
    }

    public void matchString() throws IOException, XerialException {
        match('"');

        boolean toContinue = true;
        for (;;) {
            int c = scanner.LA(1);
            switch (c) {
            case '"':
                // end of string
                consume();
                return;
            case '\\':
                // escape sequence 
                matchEscapeSequence();
                break;
            case BufferedScanner.EOF:

                throw error("String", c);
            default:
                consume();
            }
        }

    }

    public void matchUntilNewLine() throws IOException {

        for (;;) {
            int c = scanner.LA(1);
            if (c == BufferedScanner.EOF) {
                break;
            }
            consume();
        }
    }

    public void matchWhiteSpace_s() throws IOException {
        // (' ' | '\t')*
        for (;;) {
            int c = scanner.LA(1);
            if (c == ' ' || c == '\t') {
                consume();
            }
            else
                break;
        }
    }

    public void mInit() throws XerialException, IOException {
        int c = scanner.LA(1);
        switch (c) {
        case ' ':
        case '\t': {
            int indentLength = matchIndent();
            emit(new SilkIndentToken(indentLength));
            break;
        }
        case '-': {
            int LA2 = scanner.LA(2);
            if (LA2 >= '0' && LA2 <= '9') {
                matchUntilNewLine();
                emitWithText(SilkTokenType.DataLine);
                break;
            }
            else {
                // Begin node name
                consume();
                switch (scanner.LA(1)) {
                case '-':
                    consume();
                    emit(SilkTokenType.HereDocSep);
                    // next line is here doc
                    nextLineState = State.HERE_DOC;
                    break;
                case '*':
                    consume();
                    emit(SilkTokenType.BlockNode);
                    break;
                case '|':
                    consume();
                    emit(SilkTokenType.TabNode);
                    break;
                default:
                    emit(SilkTokenType.Node);
                    break;
                }
                state = State.NODE_NAME;
            }
            break;
        }
        case '>':
            consume();
            emit(SilkTokenType.SeqNode);
            state = State.NODE_NAME;
            break;
        case '#':
            // Begin comment
            consume();
            matchUntilNewLine();
            emitWithText(SilkTokenType.LineComment);
            break;
        case '%':
            // Begin preamble
            consume();
            emit(SilkTokenType.Preamble);
            state = State.NODE_NAME;
            break;
        case '@':
            // Begin function
            consume();
            emit(SilkTokenType.At);
            state = State.NODE_NAME;
            break;
        case BufferedScanner.EOF:
            emit(SilkTokenType.BlankLine);
            break;
        case '\\': {
            int c2 = scanner.LA(2);
            if (c2 == '-') {
                // '-' character is escaped to distinguish minus symbol with a node indicator
                consume();
                scanner.mark();
            }
            matchUntilNewLine();
            emitWithText(SilkTokenType.DataLine);
            break;
        }
        default:
            matchUntilNewLine();
            emitWithText(SilkTokenType.DataLine);
            break;
        }
    }

    public void mToken() throws XerialException, IOException {

        skipWhiteSpaces();

        int c = scanner.LA(1);
        switch (c) {
        case '(':
        case ')':
            consume();
            emit(c);
            state = State.ATTRIBUTE_NAME;
            break;
        case '-':
            consume();
            switch (state) {
            case NODE_NAME:
            case ATTRIBUTE_NAME:
                emit(SilkTokenType.Separator);
                state = State.ATTRIBUTE_NAME;
                break;
            default:
                // distinguish from number
                int c2 = scanner.LA(1);
                if (c2 >= '0' && c2 <= '9')
                    mNumber();
                else
                    emitTrimmed(SilkTokenType.NodeValue);
                break;
            }
            break;
        case ':':
            consume();
            emit(SilkTokenType.Colon);
            switch (state) {
            case NODE_NAME:
                state = State.NODE_VALUE;
                break;
            case ATTRIBUTE_NAME:
                state = State.ATTRIBUTE_VALUE;
                break;
            }
            break;
        case ',':
            consume();
            emit(SilkTokenType.Comma);
            switch (state) {
            case ATTRIBUTE_VALUE:
                state = State.ATTRIBUTE_NAME;
                break;
            }
            break;
        case '@':
        case '<':
        case '>':
        case '[':
        case ']':
        case '?':
        case '*':
            consume();
            emit(c);
            break;
        case '"':
            mString();
            break;
        case '+':
            consume();
            switch (state) {
            case ATTRIBUTE_VALUE:
                emitTrimmed(SilkTokenType.NodeValue);
                break;
            default:
                emit(SilkTokenType.Plus);
                break;
            }
            break;
        case BufferedScanner.EOF:
            break;
        default:
            if (c >= '0' && c <= '9') {
                mNumber();
            }
            else {
                matchQName();
                emitTrimmed(SilkTokenType.QName);
            }
            break;
        }
    }

    public void mNodeValue() throws XerialException, IOException {

        skipWhiteSpaces();

        matchUntilNewLine();
        emitTrimmed(SilkTokenType.NodeValue);
    }

    public void mNumber() throws IOException, XerialException {

        int c = scanner.LA(1);
        // Negative flag
        if (c == '-') {
            int c2 = scanner.LA(2);
            if (c2 >= '0' && c2 <= '9') {
                consume();
                c = c2;
            }
        }

        if (c == '0') {
            consume();
        }
        else if (c >= '1' && c <= '9') {
            consume();
            matchDigit_s();
        }

        c = scanner.LA(1);
        switch (c) {
        case '.': {
            consume();
            matchDigit_p();
            int c2 = scanner.LA(1);
            matchExp();
            emitWithText(SilkTokenType.Real);
            break;
        }
        default:
            if (matchExp())
                emitWithText(SilkTokenType.Real);
            else
                emitWithText(SilkTokenType.Integer);
            break;
        }
    }

    public void mString() throws XerialException, IOException {
        matchString();
        emitString();
    }

    public void skipWhiteSpaces() throws XerialException, IOException {
        matchWhiteSpace_s();
        scanner.mark();
    }

}
