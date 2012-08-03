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
// UTF8String.java
// Since: 2011/05/29 14:31:33
//
// $URL$
// $Author$
//--------------------------------------
package xerial.silk.util.io;

import java.nio.charset.Charset;

/**
 * UTF8 String
 * 
 * @author leo
 * 
 */
public class UTF8String implements CharSequence {
    public final static Charset UTF8 = Charset.forName("UTF-8");

    private final byte[] str;
    private String s;

    /**
     * cached hash code
     */
    private int hash;

    public UTF8String(byte[] str) {
        this(str, 0, str.length);
    }

    public UTF8String(byte[] input, int offset, int len) {
        this.str = new byte[len];
        System.arraycopy(input, offset, this.str, 0, len);
    }

    public UTF8String(char[] input, int offset, int len) {
        this.str = new String(input, offset, len).getBytes(UTF8);
    }

    public UTF8String(String s) {
        this.s = s;
        this.str = s.getBytes(UTF8);
    }

    public static UTF8String format(String format, Object... args) {
        return new UTF8String(String.format(format, args));
    }

    public int get(int index) {
        return str[index];
    }

    public byte[] getBytes() {
        return str;
    }

    public int byteSize() {
        return str.length;
    }

    @Override
    public String toString() {
        if (s == null) {
            s = new String(str, UTF8);
        }
        return s;
    }

    @Override
    public int hashCode() {
        int h = hash;
        int len = byteSize();
        if (h == 0 && len > 0) {
            int off = 0;
            byte val[] = str;
            for (int i = 0; i < len; i++) {
                h = 31 * h + val[off++];
            }
            hash = h;
        }
        return h;
    }

    @Override
    public boolean equals(Object anObject) {
        if (this == anObject) {
            return true;
        }
        if (anObject instanceof String) {
            UTF8String anotherString = UTF8String.class.cast(anObject);
            int n = byteSize();
            if (n == anotherString.byteSize()) {
                byte v1[] = str;
                byte v2[] = anotherString.str;
                int i = 0;
                int j = 0;
                while (n-- != 0) {
                    if (v1[i++] != v2[j++])
                        return false;
                }
                return true;
            }
        }
        return false;
    }

    @Override
    public int length() {
        return toString().length();
    }

    @Override
    public char charAt(int index) {
        return toString().charAt(index);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        return toString().subSequence(start, end);
    }

}
