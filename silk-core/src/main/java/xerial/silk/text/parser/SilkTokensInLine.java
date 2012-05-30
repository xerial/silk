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
// SilkTokensInLine.java
// Since: 2011/06/08 16:13:31
//
// $URL$
// $Author$
//--------------------------------------
package xerial.silk.text.parser;

import org.xerial.util.StringUtil;
import xerial.silk.text.parser.token.SilkToken;

import java.util.List;

/**
 * A list of tokens in a single line
 * 
 * @author leo
 * 
 */
public class SilkTokensInLine
{
    public final CharSequence    line;
    public final List<SilkToken> tokenList;
    public final SilkLexer.State nextLineState;

    public SilkTokensInLine(CharSequence line, List<SilkToken> tokenList, SilkLexer.State nextLineState) {
        this.line = line;
        this.tokenList = tokenList;
        this.nextLineState = nextLineState;
    }

    @Override
    public String toString() {
        return StringUtil.join(tokenList, "\n");
    }
}
