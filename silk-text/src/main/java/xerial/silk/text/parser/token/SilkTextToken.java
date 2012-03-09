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
// SilkTextToken.java
// Since: 2011/06/06 11:15:45
//
// $URL$
// $Author$
//--------------------------------------
package xerial.silk.text.parser.token;


/**
 * Default token type for text data
 * 
 * @author leo
 * 
 */
public class SilkTextToken extends SilkToken
{
    public final CharSequence text;

    public SilkTextToken(SilkTokenType type, CharSequence text, int posInLine) {
        super(type, posInLine);
        this.text = text;
    }

    @Override
    public CharSequence getText() {
        return text;
    }

    public static String toVisibleString(CharSequence s) {
        if (s == null)
            return "";
        String text = s.toString();
        text = text.replaceAll("\n", "\\\\n");
        text = text.replaceAll("\r", "\\\\r");
        text = text.replaceAll("\t", "\\\\t");
        return text;
    }

    @Override
    public String toString() {
        return String.format("pos:%2d [%s] %s", posInLine, type.name(), toVisibleString(getText()));
    }

    public static String chompNewLine(String line) {
        return line.replaceAll("(\r|\n\r?)$", "");
    }

}
