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
package xerial.silk.macros

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import xerial.lens.ObjectSchema

import scala.language.existentials
import scala.language.experimental.macros
import scala.reflect.ClassTag
import scala.reflect.macros.blackbox.Context

/**
 *
 */
object SilkMacros {


  def mTaskCommand[B:c.WeakTypeTag](c: Context)(block:c.Tree) = {
    import c.universe._
    q"new xerial.silk.core.TaskDef(xerial.silk.core.TaskContext(${fc(c)}))($block)"
  }

  def mShellCommand(c: Context)(args: c.Tree*) = {
    import c.universe._
   q"xerial.silk.core.shell.ShellCommand(xerial.silk.core.TaskContext(${fc(c)}, Seq(..$args).collect{case f:xerial.silk.core.SilkOp[_] => f}), ${c.prefix.tree}.sc, Seq(..$args))"
  }

  def mToSilk(c: Context) = {
    import c.universe._
    q"xerial.silk.core.MultipleInputs(xerial.silk.core.TaskContext(${fc(c)}, ${c.prefix.tree}.s))"
  }

  def mNewFrame[A: c.WeakTypeTag](c: Context)(in: c.Expr[Seq[A]]) = {
    import c.universe._
    q"xerial.silk.core.InputFrame(xerial.silk.core.TaskContext(${fc(c)}), $in)"
  }

  def mFileInput(c:Context)(in:c.Expr[File]) = {
    import c.universe._
    q"xerial.silk.core.FileInput(xerial.silk.core.TaskContext(${fc(c)}), $in)"
  }

  private val counter = new AtomicInteger(0)
  def getNewName() : String = s"t${counter.getAndIncrement()}"

  def fc(c: Context) = new MacroHelper[c.type](c).createOpRef

  class MacroHelper[C <: Context](val c: C) {
    import c.universe._
    /**
     * Find a function/variable/class context where the expression is used
     * @return
     */
    def createOpRef: c.Expr[SourceRef] = {
      // Find the enclosing method.
      val owner = c.internal.enclosingOwner
      val name = if(owner.fullName.endsWith("$anonfun")) {
        owner.fullName.replaceAll("\\$anonfun$", "") + getNewName()
      }
      else {
        owner.fullName
      }

      val selfCl = c.Expr[AnyRef](This(typeNames.EMPTY))
      val pos = c.enclosingPosition
      c.Expr[SourceRef](q"xerial.silk.macros.SourceRef($selfCl.getClass, ${name}, ${pos.source.path}, ${pos.line}, ${pos.column})")
    }
  }

}
