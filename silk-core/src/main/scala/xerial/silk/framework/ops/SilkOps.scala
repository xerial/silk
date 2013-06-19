//--------------------------------------
//
// SilkOps.scala
// Since: 2013/06/16 16:13
//
//--------------------------------------

package xerial.silk.framework.ops

import scala.language.existentials
import scala.language.experimental.macros
import scala.reflect.ClassTag
import xerial.lens.{Parameter, ObjectSchema}
import xerial.core.log.Logger
import java.util.UUID
import scala.reflect.runtime.{universe=>ru}
import java.io._
import xerial.silk.core.{SilkFlow, ShellCommand}



