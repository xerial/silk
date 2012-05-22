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

import java.io._


//--------------------------------------
//
// FileSource.scala
// Since: 2012/05/22 20:10
//
//--------------------------------------

/**
 * Utilities for reading/writing files
 * @author leo
 */
object FileSource {

  implicit def strToFile(s:String) = new File(s)

  def write(file:File)(f:OutputStream => Unit) {
    val out = new BufferedOutputStream(new FileOutputStream(file))
    try {
      f(out)
    }
    finally {
      out.flush
      out.close
    }
  }

  def read(file:File)(f:InputStream => Unit) {
    val in = new BufferedInputStream(new FileInputStream(file))
    try {
      f(in)
    }
    finally {
      in.close
    }
  }

}