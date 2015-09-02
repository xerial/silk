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
package xerial.silk.core.io

import xerial.core.log.Logger
import xerial.silk._

object ServiceGuard {

  def empty = new ServiceGuard[Int] {
    def close = {
      // do nothing
    }
    protected[silk] val service : Int = 0
  }

}

/**
 * A created service will be closed after the for-comprehension
 * {{{
 *   for(a <- service) {
 *     // do something
 *   } // service will be closed
 * }}}
 * @author Taro L. Saito
 */
trait ServiceGuard[Service] extends Iterable[Service] with Logger { self =>

  def close: Unit

  protected[silk] val service : Service

  def iterator = NA

  def wrap[R](f: Service => R) : R = {
    try {
      f(service)
    }
    finally {
      close
    }
  }

  override def foreach[U](f:Service=>U) { wrap(f) }
//  def map[B](f:Service => B) = new ServiceGuard[B] {
//    override def wrap[R](g:B => R) : R = {
//      try {
//        f
//      }
//      finally {
//        self.close
//
//      }
//    }
//  }
//  def map[R](f:Service => R) : R = { wrap(f) }
//  def flatMap[R](f:Service => R) : ServiceGuard[R] = new ServiceGuard[R] {
//    override def
//  }

  def whenMissing[B](f: => B) : self.type = { self }
}

class MissingService[Service] extends ServiceGuard[Service] { self =>
  def close {}
  protected[silk] val service : Service = null.asInstanceOf[Service]


  override def foreach[U](f:Service=>U) {
  // do nothing
  }

  override def whenMissing[B](f: => B) = { f; self }

}
