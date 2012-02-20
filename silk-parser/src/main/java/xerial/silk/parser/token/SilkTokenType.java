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
// SilkTokenType.java
// Since: 2011/05/02 12:03:46
//
// $URL$
// $Author$
//--------------------------------------
package xerial.silk.parser.token;

import xerial.silk.io.UTF8String;

import java.util.HashMap;

public enum SilkTokenType {
    Error,
    EOF,
    Unknown,
    Indent,
    Node("-"),
    TabNode("-|"),
    BlockNode("-*"),
    SeqNode("->"),
    HereDocSep("--"),
    LineComment("#"),
    NewLine,
    QName,
    Preamble("%"),
    At("@"),
    Colon(":"),
    Comma(","),
    Asterisk("*"),
    Plus("+"),
    Question("?"),
    String,
    Integer,
    Real,
    Separator("-"),
    LParen("("),
    RParen(")"),
    LBracket("["),
    RBracket("]"),
    LSquare("<"),
    RSquare(">"),
    DataLine,
    HereDoc,
    BlankLine,
    NodeValue

    ;

    private final String                           symbol;
    private final CharSequence                     rawText;

    private static HashMap<Integer, SilkTokenType> symbolTable = new HashMap<Integer, SilkTokenType>();

    static {
        for (SilkTokenType each : SilkTokenType.values()) {
            if (each.symbol != null && each.symbol.length() == 1) {
                int ch = each.symbol.charAt(0);
                symbolTable.put(ch, each);
            }
        }
    }

    private SilkTokenType() {
        this.symbol = "";
        this.rawText = new UTF8String(this.symbol);
    }

    private SilkTokenType(String symbol) {
        this.symbol = symbol;
        this.rawText = new UTF8String(this.symbol);
    }

    public CharSequence toRawString() {
        return rawText;
    }

    @Override
    public java.lang.String toString() {
        return symbol;
    }

    public static SilkTokenType toTokenType(int symbol) {
        return symbolTable.get(symbol);
    }

}
