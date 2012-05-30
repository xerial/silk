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
// BufferedScanner.java
// Since: 2011/04/30 15:36:32
//
// $URL$
// $Author$
//--------------------------------------
package xerial.silk.util.io;

import org.xerial.util.ArrayDeque;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

/**
 * A character scanner with look-ahead and buffer mark functions.
 * 
 * @author leo
 * 
 */
public class BufferedScanner {

    private static interface Buffer {
        int length();

        int get(int index);

        int feed(int offset, int length) throws IOException;

        CharSequence toRawString(int offset, int length);

        void close() throws IOException;

        /**
         * Slide the buffer contents [offset, offset+len) to [0,.. len)
         * 
         * @param offset
         * @param len
         */
        void slide(int offset, int len);

        ArrayBuilder newBuilder(int initialBufferSize);

        //byte[] rawBuffer();
    }

    private static interface ArrayBuilder {
        void append(Buffer buf, int offset, int length);

        CharSequence toRawString();

        int size();
    }

    private static class ByteArrayBuilder implements ArrayBuilder {

        ByteArrayOutputStream out;

        public ByteArrayBuilder(int initialBufferSize) {
            this.out = new ByteArrayOutputStream(initialBufferSize);
        }

        @Override
        public void append(Buffer buf, int offset, int length) {
            out.write(((ByteBuffer) buf).buffer, offset, length);
        }

        @Override
        public CharSequence toRawString() {
            return new UTF8String(out.toByteArray());
        }

        @Override
        public int size() {
            return out.size();
        }
    }

    private static class CharArrayBuilder implements ArrayBuilder {

        StringBuilder out;

        public CharArrayBuilder(int initialBufferSize) {
            this.out = new StringBuilder(initialBufferSize);
        }

        @Override
        public void append(Buffer buf, int offset, int length) {
            out.append(((CharBuffer) buf).buffer, offset, length);
        }

        @Override
        public CharSequence toRawString() {
            return new UTF8String(out.toString());
        }

        @Override
        public int size() {
            return out.length();
        }

    }

    private static class ByteBuffer implements Buffer {
        InputStream source;
        byte[] buffer;

        public ByteBuffer(InputStream source, int bufferSize) {
            this.source = source;
            this.buffer = new byte[bufferSize];
        }

        public ByteBuffer(byte[] buffer) {
            this.buffer = buffer;
        }

        @Override
        public int get(int index) {
            return buffer[index];
        }

        @Override
        public int length() {
            return buffer.length;
        }

        @Override
        public CharSequence toRawString(int offset, int length) {
            return new UTF8String(buffer, offset, length);
        }

        @Override
        public int feed(int offset, int length) throws IOException {
            if (source == null)
                return -1;
            return source.read(buffer, offset, length);
        }

        @Override
        public void close() throws IOException {
            if (source != null)
                source.close();
        }

        @Override
        public void slide(int offset, int len) {
            System.arraycopy(buffer, offset, buffer, 0, len);
        }

        @Override
        public ArrayBuilder newBuilder(int initialBufferSize) {
            return new ByteArrayBuilder(initialBufferSize);
        }

    }

    private static class CharBuffer implements Buffer {
        Reader source;
        char[] buffer;

        public CharBuffer(Reader source, int bufferSize) {
            this.source = source;
            this.buffer = new char[bufferSize];
        }

        public CharBuffer(CharSequence s) {
            this.source = null;
            this.buffer = new char[s.length()];
            for (int i = 0; i < s.length(); ++i)
                buffer[i] = s.charAt(i);
        }

        @Override
        public int get(int index) {
            return buffer[index];
        }

        @Override
        public int length() {
            return buffer.length;
        }

        @Override
        public int feed(int offset, int length) throws IOException {
            if (source == null)
                return -1;
            return source.read(buffer, offset, length);
        }

        @Override
        public void close() throws IOException {
            if (source != null)
                source.close();
        }

        @Override
        public void slide(int offset, int len) {
            System.arraycopy(buffer, offset, buffer, 0, len);
        }

        @Override
        public CharSequence toRawString(int offset, int length) {
            return new String(buffer, offset, length);
        }

        @Override
        public ArrayBuilder newBuilder(int initialBufferSize) {
            return new CharArrayBuilder(initialBufferSize);
        }
    }

    public final static int EOF = -1;
    private Buffer buffer;
    private int bufferLimit = 0;
    private boolean reachedEOF = false;

    private static class ScannerState {
        public int cursor = 0;

        public ScannerState() {}

        public ScannerState(ScannerState m) {
            this.cursor = m.cursor;
        }

        @Override
        public String toString() {
            return String.format("%d", cursor);
        }
    }

    private ArrayDeque<ScannerState> markQueue = new ArrayDeque<ScannerState>();
    private ScannerState current;

    public BufferedScanner(InputStream in) {
        this(in, 8 * 1024); // 8kb buffer
    }

    public BufferedScanner(InputStream in, int bufferSize) {
        this(new ByteBuffer(in, bufferSize));
        if (in == null)
            throw new NullPointerException("input is null");
    }

    public BufferedScanner(Reader in) {
        this(in, 8 * 1024); // 8kb buffer
    }

    public BufferedScanner(Reader in, int bufferSize) {
        this(new CharBuffer(in, bufferSize));
        if (in == null)
            throw new NullPointerException("input is null");
    }

    public BufferedScanner(Buffer buffer) {
        this.buffer = buffer;
        this.current = new ScannerState();
    }

    public BufferedScanner(CharSequence s) {
        if (s.getClass() == UTF8String.class)
            this.buffer = new ByteBuffer(((UTF8String) s).getBytes());
        else
            this.buffer = new CharBuffer(s);
        this.bufferLimit = this.buffer.length();
        this.current = new ScannerState();
        this.reachedEOF = true;
    }

    public BufferedScanner(String s) {
        this(new ByteBuffer(s.getBytes()));
        this.bufferLimit = buffer.length();
        this.reachedEOF = true;
    }

    public BufferedScanner(UTF8String s) {
        this(new ByteBuffer(s.getBytes()));
        this.bufferLimit = buffer.length();
        this.reachedEOF = true;
    }

    public void close() throws IOException {
        buffer.close();
    }

    public boolean hasReachedEOF() {
        return reachedEOF && current.cursor >= bufferLimit;
    }

    public CharSequence nextLine() throws IOException {
        ArrayBuilder currentLine = null;
        for (;;) {
            if (current.cursor >= bufferLimit)
                fill();
            if (current.cursor >= bufferLimit) {
                if (currentLine != null && currentLine.size() > 0)
                    return currentLine.toRawString();
                else
                    return null;
            }
            boolean eol = false;
            int i = current.cursor;
            int ch = EOF;
            charLoop: for (; i < bufferLimit; i++) {
                ch = buffer.get(i);
                if (ch == '\n' || ch == '\r') {
                    eol = true;
                    break charLoop;
                }
            }
            int start = current.cursor;
            int len = i - start;
            current.cursor = i + 1;
            if (eol) {
                if (ch == '\r') {
                    if (LA(1) == '\n') {
                        current.cursor++;
                    }
                }

                if (currentLine == null) {
                    //incrementLineCount();
                    return buffer.toRawString(start, len);
                }
                currentLine.append(buffer, start, len);
                //incrementLineCount();
                return currentLine != null && currentLine.size() > 0 ? currentLine.toRawString()
                        : null;
            }
            if (currentLine == null)
                currentLine = buffer.newBuilder(16);
            currentLine.append(buffer, start, len);
        }
    }

    public int consume() throws IOException {
        if (current.cursor >= bufferLimit) {
            if (!fill()) {
                // No more characters to consume. Do nothing
                return EOF;
            }
        }
        int ch = buffer.get(current.cursor++);
        return ch;
    }

    /**
     * Get the next character of lookahead.
     * 
     * @param lookahead
     * @return
     * @throws IOException
     */
    public int LA(int lookahead) throws IOException {
        if (current.cursor + lookahead - 1 >= bufferLimit) {
            if (!fill()) {
                // No more character
                return EOF;
            }
        }
        // current.cursor might be changed at fill(), so we need to recompute the lookahead position  
        return buffer.get(current.cursor + lookahead - 1);
    }

    private boolean fill() throws IOException {
        if (reachedEOF) {
            return false;
        }
        // Move the [mark ... limit)
        if (!markQueue.isEmpty()) {
            ScannerState mark = markQueue.peekFirst();
            int lenToPreserve = bufferLimit - mark.cursor;
            if (lenToPreserve < buffer.length()) {
                // Move [mark.cursor, limit) to the [0, ..., mark.cursor)
                if (lenToPreserve > 0)
                    buffer.slide(mark.cursor, lenToPreserve);
                bufferLimit = lenToPreserve;
                current.cursor -= mark.cursor;
                int slideLen = mark.cursor;
                for (ScannerState each : markQueue) {
                    each.cursor -= slideLen;
                }
            }
            else {
                // The buffer got too big, invalidate the mark
                markQueue.clear();
                bufferLimit = 0;
                current.cursor = 0;
            }
        }
        else {
            bufferLimit = 0;
            current.cursor = 0;
        }
        // Read the data from the stream, and fill the buffer
        int readLen = buffer.length() - bufferLimit;
        int readBytes = buffer.feed(current.cursor, readLen);
        if (readBytes < readLen) {
            reachedEOF = true;
        }
        if (readBytes == -1)
            return false;
        else {
            bufferLimit += readBytes;
            return true;
        }

    }

    public Range getSelectedRange() {
        if (markQueue.isEmpty())
            throw new NullPointerException("no mark is set");
        return new Range(markQueue.getLast().cursor, current.cursor);
    }

    public CharSequence selectedRawString() {
        Range r = getSelectedRange();
        return buffer.toRawString(r.begin, r.size());
    }

    public CharSequence selectedRawStringWithTrimming() {
        Range r = trim(getSelectedRange());
        return buffer.toRawString(r.begin, r.size());
    }

    private static class Range {
        public final int begin;
        public final int end;

        public Range(int begin, int end) {
            this.begin = begin;
            this.end = end;
        }

        public int size() {
            return end - begin;
        }

        @Override
        public String toString() {
            return String.format("[%d,%d)", begin, end);
        }
    }

    Range trim(Range input) {
        int begin = input.begin;
        int end = input.end;
        for (; begin < end; begin++) {
            int c = buffer.get(begin);
            if (c != ' ' && c != '\t' && c != '\r' && c != '\n')
                break;
        }
        for (; begin < end; end--) {
            int c = buffer.get(end - 1);
            if (c != ' ' && c != '\t' && c != '\r' && c != '\n')
                break;
        }
        if (begin >= end) {
            begin = end;
        }
        int size = end - begin;
        return new Range(begin, end);
    }

    Range trim() {
        return trim(getSelectedRange());
    }

    public CharSequence selectedRawString(int margin) {
        if (markQueue.isEmpty())
            return null;
        Range selected = getSelectedRange();
        Range trimmed = new Range(selected.begin + margin, selected.end - margin);
        return buffer.toRawString(trimmed.begin, trimmed.size());
    }

    public CharSequence selectedRawStringFromFirstMark() {
        Range selected = new Range(markQueue.peekFirst().cursor, current.cursor);
        return buffer.toRawString(selected.begin, selected.size());
    }

    public int distanceFromFirstMark() {
        return current.cursor - markQueue.peekFirst().cursor;
    }

    public void mark() {
        markQueue.add(new ScannerState(current));
    }

    public void resetMarks() {
        markQueue.clear();
    }

    /**
     * Reset the stream position to the last marker.
     */
    public void rewind() {
        current = markQueue.pollLast();
    }

}
