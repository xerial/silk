//--------------------------------------
//
// MacroUtil.scala
// Since: 2013/05/23 10:20
//
//--------------------------------------

package xerial.silk.util

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