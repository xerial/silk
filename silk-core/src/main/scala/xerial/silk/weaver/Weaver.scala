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
package xerial.silk.weaver

import xerial.core.log.Logger
import xerial.silk.core.Task

/**
 * Weaver is an interface for evaluating Silk operations.
 * InMemoryWeaver, LocalWeaver, ClusterWeaver
 *
 * @author Taro L. Saito
 */
trait Weaver extends Logger {

  /**
   * Custom configuration type that is specific to the Weaver implementation
   * For example, if one needs to use local weaver, only the LocalConfig type will be set to this type.
   */
  type Config

  /**
   * Configuration object
   */
  val config: Config


  def weave(op: Task): Unit = {
    debug(s"op:\n${op}")

    // TODO inject optimizer
    val optimizer = new SequentialOptimizer(Seq.empty)
    val optimized = optimizer.transform(op)
    debug(s"optimized op:\n${optimized}")

    eval(optimized)
  }

  def eval(op: Task, level: Int = 0): Unit

  protected def indent(level: Int): String = {
    if (level > 0) {
      (0 until level).map(i => " ").mkString
    }
    else {
      ""
    }
  }

}

