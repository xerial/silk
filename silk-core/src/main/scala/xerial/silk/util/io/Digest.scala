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

package xerial.silk.io

import java.security.MessageDigest
import java.io.InputStream
import xerial.silk.util.io.PageInputStream
import xerial.silk.util.Logging

//--------------------------------------
//
// Digest.scala
// Since: 2012/02/22 10:38
//
//--------------------------------------

/**
 * @author leo
 */
object Digest extends Logging {
  
  def md5sum(data: TraversableOnce[Array[Byte]]) : String =
    digest(data, MessageDigest.getInstance("md5"))

  def md5sum(data: InputStream) : String =
    md5sum(new PageInputStream(data))

  def sha1sum(data:TraversableOnce[Array[Byte]]) : String =
    digest(data, MessageDigest.getInstance("sha1"))

  def sha1sum(data: InputStream) : String =
    sha1sum(new PageInputStream(data))

  def digest(data:TraversableOnce[Array[Byte]], digest:MessageDigest) : String = {
    var dataSize = 0
    val d = data.foldLeft(digest){
      (digest, data) =>
        digest.update(data)
        dataSize += data.length
        digest
    }
    //debug("data length:%,d", dataSize)
    toHEXString(d.digest())
  }
  

  def toHEXString(byte:Array[Byte]) : String = byte.map((n: Byte) => "%02x".format(n & 0xff)).mkString
  
}