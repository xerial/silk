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

package xerial.silk.util

import java.util.regex.Pattern
import collection.mutable.{ArrayBuffer, ArrayBuilder, WeakHashMap}

//--------------------------------------
//
// CName.scala
// Since: 2012/02/16 14:58
//
//--------------------------------------

/**
 * Utility for combining names in different formats. For example,
 * a variable name, localAddress, can be written as "local address", "local_address", etc.
 *
 * CanonicalName is the representative name of these name variants.
 *
 * <pre>
 * CName("localAddress") == CName("local address") == CName("local_address")
 * </pre>
 *
 *
 * @author leo
 */
object CName {
//  def apply(name: String): CName = {
//
//  }

  private val paramNameReplacePattern = Pattern.compile("[\\s-_]");
  private val canonicalNameTable = new WeakHashMap[String, String]
  private val naturalNameTable = new WeakHashMap[String, String]

  def toCanonicalName(paramName: String): String = {
    if (paramName == null)
      return paramName;

    canonicalNameTable.getOrElse(paramName, {
      val m = paramNameReplacePattern.matcher(paramName);
      val cName = m.replaceAll("").toLowerCase();
      canonicalNameTable.put(paramName, cName);
      cName
    })
  }

  def toNaturalName(varName: String): String = {
    if (varName == null)
      return null;

    def isSplitChar(c: Char) = c.isUpper || c == '_' || c == '-' || c == ' '
    def isUpcasePrefix(c: Char) = c.isUpper || c.isDigit


    def translate(varName: String) = {
      var components = Array[String]()
      var start = 0;
      var cursor = 0;

      def skipUpcasePrefix: Unit =
        if (cursor < varName.length && isUpcasePrefix(varName(cursor))) {
          cursor += 1
          skipUpcasePrefix
        }

      // Upcase prefix length is longer than or equals to 2
      def hasUpcasePrefix: Boolean = (cursor - start) >= 2

      def parseWikiComponent : Unit = {
        if(cursor < varName.length && !isSplitChar(varName(cursor))) {
          cursor += 1
          parseWikiComponent
        }
      }

      def findWikiComponent : Unit = {
        if(cursor < varName.length) {
          skipUpcasePrefix
          if (hasUpcasePrefix) {
            components = components :+ varName.substring(start, cursor)
            start = cursor
          }
          else {
            parseWikiComponent
            if (start < cursor)
              components = components :+ varName.substring(start, cursor).toLowerCase()
            else
              cursor += 1
            start = cursor
          }
          findWikiComponent
        }
      }

      findWikiComponent
      val nName = components.mkString(" ")
      nName
    }

    naturalNameTable.getOrElse(varName, translate(varName))
  }

}

class CName {

}