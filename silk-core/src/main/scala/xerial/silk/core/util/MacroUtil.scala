/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//--------------------------------------
//
// MacroUtil.scala
// Since: 2013/05/23 10:20
//
//--------------------------------------

package xerial.silk.core.util

import scala.reflect.runtime.{universe=>ru}

/**
 * Utilities for scala macros
 * @author Taro L. Saito
 */
object MacroUtil {

  val mirror = ru.runtimeMirror(Thread.currentThread().getContextClassLoader)

  import scala.tools.reflect.ToolBox
  val toolbox : ToolBox[ru.type] = mirror.mkToolBox()

}