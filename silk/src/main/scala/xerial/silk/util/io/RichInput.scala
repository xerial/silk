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

package xerial.silk.util.io

import java.io.InputStream
import java.io.Reader
import xerial.core.log.Logging

//--------------------------------------
//
// RichInput.scala
// Since: 2012/03/14 22:14
//
//--------------------------------------

/**
 * Enhances InputStream or Reader for block-wise reading
 * @author leo
 */
abstract class RichInput[@specialized(Byte,Char) T]()(implicit m: ClassManifest[T]) extends Logging {
  var reachedEOF = false

  def read(b: Array[T], off: Int, len: Int): Int
  def newArray(size:Int) : Array[T] = new Array[T](size)

  def readFully(b: Array[T], off: Int, len: Int): Int = {
    def loop(count: Int): Int = {
      if (count >= len)
        count
      else {
        val readLen = read(b, off + count, len - count)
        if (readLen == -1) {
          reachedEOF = true
          count
        }
        else
          loop(count + readLen)
      }
    }

    loop(0)
  }

  def readFully(b: Array[T]): Int = {
    readFully(b, 0, b.length)
  }

}

class RichInputStream(in:InputStream) extends RichInput[Byte] {
  def read(b:Array[Byte], off:Int, len:Int) = in.read(b, off, len)
}

class RichReader(in:Reader) extends RichInput[Char] {
  def read(b:Array[Char], off:Int, len:Int) = in.read(b, off, len)
}

