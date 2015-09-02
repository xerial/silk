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
// LazyF0.scala
// Since: 2013/05/08 2:46 PM
//
//--------------------------------------

package xerial.silk.core.closure

object LazyF0 {
  def apply[R](f: => R) = new LazyF0(f)
}


/**
 * This class is used to obtain the class names of the call-by-name functions (Function0[R]).
 *
 * This wrapper do not directly access the field f (Function0[R]) in order
 * to avoid the evaluation of the function.
 * @param f
 * @tparam R
 */
class LazyF0[R](f: => R) {

  /**
   * Obtain the function class
   * @return
   */
  def functionClass: Class[_] = {
    val field = this.getClass.getDeclaredField("f")
    field.get(this).getClass
  }

  def functionInstance: Function0[R] = {
    this.getClass.getDeclaredField("f").get(this).asInstanceOf[Function0[R]]
  }

  /**
   * We never use this method, but this definition is necessary in order to let the compiler generate the private field 'f' that
   * holds a reference to the call-by-name function.
   * @return
   */
  def eval = f
}
