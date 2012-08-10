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

package xerial.silk.text.parser

import xerial.silk.model.SilkRecord

//--------------------------------------
//
// SilkElement.scala
// Since: 2012/08/10 0:15
//
//--------------------------------------

/**
 * @author leo
 */
sealed abstract class SilkElement

object SilkElement {
  case class Preamble(name:String, params:IndexedSeq[PreambleParam]) extends SilkElement
  case class PreambleParam(name:String, value:String)
  case class DLine(line:CharSequence) extends SilkElement
  case class Node(level:Int, name:Option[CharSequence], params:Seq[Node], value:Option[CharSequence]) extends SilkElement
}


