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
// ClosureCleaner.scala
// Since: 2013/11/06 15:00
//
//--------------------------------------

package xerial.silk.core.closure

import xerial.core.log.Logger
import xerial.silk.core.{SilkTransformer, Silk}
import scala.collection.GenTraversableOnce

/**
 * @author Taro L. Saito
 */
object ClosureCleaner extends Logger {

  def clean[A](op:Silk[A]) : Silk[A] = {
    val t = new SilkTransformer {
      override def transformParam[A](param:A) = {
        param match {
          case s:GenTraversableOnce[_] => s.asInstanceOf[A]
          case f1:Function1[_, _] =>
            trace(s"cleanup ${f1.getClass}")
            ClosureSerializer.cleanupF1(f1).asInstanceOf[A]
          case _ => param
        }
      }
      def transformSilk[A](op:Silk[A]) = op
    }

    t.transform(op, isRecursive = false)
  }

}