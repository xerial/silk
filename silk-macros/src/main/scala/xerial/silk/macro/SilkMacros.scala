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

import scala.language.existentials
import scala.language.experimental.macros
import scala.reflect.macros.blackbox.Context

/**
 *
 */
object SilkMacros {

  def mShellCommand(c: Context)(args: c.Tree*) = {
    import c.universe._
    q"xerial.silk.core.shell.ShellCommand(xerial.silk.core.TaskContext(${fc(c)}, Seq(..$args).collect{case f:xerial.silk.core.SilkOp[_] => f}), ${c.prefix.tree}.sc, Seq(..$args))"
  }

  def mDatabaseOpen(c:Context) = {
    import c.universe._
    q"xerial.silk.core.DBRef(xerial.silk.core.TaskContext(${fc(c)}), ${c.prefix.tree}, xerial.silk.core.Open)"
  }

  def mTableOpen(c:Context)(name:c.Tree) = {
    import c.universe._
    q"xerial.silk.core.TableRef(xerial.silk.core.TaskContext(${fc(c)}, ${c.prefix.tree}), ${c.prefix.tree}, xerial.silk.core.Open, $name)"
  }

  def mTableCreate(c:Context)(name:c.Tree, colDef:c.Tree) = {
    import c.universe._
    q"xerial.silk.core.TableRef(xerial.silk.core.TaskContext(${fc(c)}, ${c.prefix.tree}), ${c.prefix.tree}, xerial.silk.core.Create, $name, $colDef)"
  }

  def mTableCreateIfNotExists(c:Context)(name:c.Tree, colDef:c.Tree) = {
    import c.universe._
    q"xerial.silk.core.TableRef(xerial.silk.core.TaskContext(${fc(c)}, ${c.prefix.tree}), ${c.prefix.tree}, xerial.silk.core.Create(true), $name, $colDef)"
  }

  def mTableDrop(c:Context)(name:c.Tree) = {
    import c.universe._
    q"xerial.silk.core.TableRef(xerial.silk.core.TaskContext(${fc(c)}, ${c.prefix.tree}), ${c.prefix.tree}, xerial.silk.core.Drop, $name)"
  }

  def mTableDropIfExists(c:Context)(name:c.Tree) = {
    import c.universe._
    q"xerial.silk.core.TableRef(xerial.silk.core.TaskContext(${fc(c)}, ${c.prefix.tree}), ${c.prefix.tree}, xerial.silk.core.Drop(true), $name)"
  }

  def mSQL(c:Context)(sql:c.Tree) = {
    import c.universe._
    q"xerial.silk.core.SQLOp(xerial.silk.core.TaskContext(${fc(c)}, ${c.prefix.tree}), ${c.prefix.tree}, ${sql})"
  }

  /**
   * Generating a new InputFrame[A] from Seq[A]
   * @return
   */
  def mNewFrame[A: c.WeakTypeTag](c: Context)(in: c.Expr[Seq[A]]) = {
    import c.universe._
    q"xerial.silk.core.InputFrame(xerial.silk.core.TaskContext(${fc(c)}), $in)"
  }

  def mFileInput[A:c.WeakTypeTag](c:Context)(in:c.Expr[File]) = {
    import c.universe._
    q"xerial.silk.core.FileInput(xerial.silk.core.TaskContext(${fc(c)}), $in)"
  }


  def mRawSQL(c: Context)(args: c.Tree*) = {
    import c.universe._
    q"xerial.silk.core.RawSQL(xerial.silk.core.TaskContext(${fc(c)}, Seq(..$args).collect{case f:xerial.silk.core.Frame[_] => f}), ${c.prefix.tree}, Seq(..$args))"
  }

  def mToSilk(c: Context) = {
    import c.universe._
    q"xerial.silk.core.MultipleInputs(xerial.silk.core.TaskContext(${fc(c)}, ${c.prefix.tree}.s))"
  }


  def mAs[A: c.WeakTypeTag](c: Context) = {
    import c.universe._
    q"xerial.silk.core.CastAs(xerial.silk.core.TaskContext(${fc(c)}, ${c.prefix.tree}))"
  }

  def mFilter[A: c.WeakTypeTag](c: Context)(condition: c.Tree) = {
    import c.universe._
    q"xerial.silk.core.FilterOp(xerial.silk.core.TaskContext(${fc(c)}, ${c.prefix.tree}), ${condition})"
  }

  def mSelect[A: c.WeakTypeTag](c: Context)(cols: c.Tree*) = {
    import c.universe._
    q"xerial.silk.core.ProjectOp(xerial.silk.core.TaskContext(${fc(c)}, ${c.prefix.tree}), Seq(..$cols))"
  }

  def mLimit[A: c.WeakTypeTag](c: Context)(rows: c.Tree) = {
    import c.universe._
    q"xerial.silk.core.LimitOp(xerial.silk.core.TaskContext(${fc(c)}, ${c.prefix.tree}), ${rows}, 0)"
  }

  def mLimitWithOffset[A: c.WeakTypeTag](c: Context)(rows: c.Expr[Int], offset: c.Expr[Int]) = {
    import c.universe._

    q"xerial.silk.core.LimitOp(xerial.silk.core.TaskContext(${fc(c)}, ${c.prefix.tree}), ${rows}, ${offset})"
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
