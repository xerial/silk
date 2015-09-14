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

import xerial.silk.core.SilkOp

/**
 *
 */
trait StaticOptimizer {
  def transform(input:SilkOp[_]) : SilkOp[_]
}

case class SequentialOptimizer(optimizers:Seq[StaticOptimizer]) extends StaticOptimizer {
  def transform(frame:SilkOp[_]) : SilkOp[_] = {
    var in = frame
    for(opt <- optimizers) {
      in = opt.transform(in)
    }
    in
  }
}


