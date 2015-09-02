package xerial.silk.core

/*
 * Copyright 2012 Taro L. Saito
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.ByteArrayOutputStream

import org.scalatest._
import xerial.core.io.Resource
import xerial.core.log.Logger
import xerial.core.util.Timer
import xerial.silk.core.util.Log4jUtil

import scala.language.implicitConversions

//--------------------------------------
//
// SilkFlatSpec.scalacala
// Since: 2012/01/09 8:45
//
//--------------------------------------

///**
// * Test case generation helper
// * @author leo
// */
//trait SilkFlatSpec extends FlatSpec with ShouldMatchers with MustMatchers with GivenWhenThen with Logger{
//
//}

trait SilkSpec extends WordSpec with Matchers with GivenWhenThen with OptionValues with Resource with Timer with Logger with BeforeAndAfter {

  Log4jUtil.suppressLog4jwarning


  implicit def toTag(t:String) = Tag(t)

  /**
   * Captures the output stream and returns the printed messages as a String
   * @param body
   * @tparam U
   * @return
   */
  def captureOut[U](body: => U) : String = {
    val out = new ByteArrayOutputStream
    Console.withOut(out) {
      body
    }
    new String(out.toByteArray)
  }

  def captureErr[U](body: => U) : String = {
    val out = new ByteArrayOutputStream
    Console.withErr(out) {
      body
    }
    new String(out.toByteArray)
  }

}
