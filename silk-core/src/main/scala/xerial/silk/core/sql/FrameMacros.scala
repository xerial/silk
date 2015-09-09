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
package xerial.silk.core.sql

import scala.reflect.macros.blackbox.Context

/**
 *
 */
object FrameMacros {

  import xerial.silk.core.SilkMacros._

  def mOpen(c:Context)(path:c.Tree) = {
    import c.universe._
    q"xerial.silk.core.sql.DBRef(${fc(c)}, xerial.silk.weaver.jdbc.sqlite.SQLite(${path}), xerial.silk.core.sql.Open)"
  }

  def mCreate(c:Context)(path:c.Tree) = {
    import c.universe._
    q"xerial.silk.core.sql.DBRef(${fc(c)}, xerial.silk.weaver.jdbc.sqlite.SQLite(${path}), xerial.silk.core.sql.Create(true))"
  }

  def mDelete(c:Context)(path:c.Tree) = {
    import c.universe._
    q"xerial.silk.core.sql.DBRef(${fc(c)}, xerial.silk.weaver.jdbc.sqlite.SQLite(${path}), xerial.silk.core.sql.Drop(true))"
  }


}
