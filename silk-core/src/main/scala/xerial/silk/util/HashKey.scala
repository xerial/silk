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

import xerial.silk.lens.ObjectSchema

//--------------------------------------
//
// HashKey.scala
// Since: 2012/04/02 15:47
//
//--------------------------------------

/**
 * Add hashing support [Any#hashCode] and [Any#equals] to an arbitrary class
 *
 * @author leo
 */
trait HashKey {
  override lazy val hashCode = {
    val schema = ObjectSchema(this.getClass)
    val hash = schema.parameters.foldLeft(17)((hash, p) =>
      hash * 31 + p.get(this).hashCode()
    )
    hash % 1907
  }

  override def equals(other: Any) = {
    if (other != null && this.getClass == other.getClass) {
      if (this eq other.asInstanceOf[AnyRef]) // if two object refs are identical
        true
      else {
        val schema = ObjectSchema(this.getClass)
        // search for non-equal parameters
        val eq = schema.parameters.find(p =>
          !p.get(this).equals(p.get(other))
        )
        // if isEmpty is true, all parameters are the same
        eq.isEmpty
      }
    }
    else
      false
  }
}