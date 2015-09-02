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

package xerial.silk.core

import xerial.core.util.CName

//--------------------------------------
//
// SilkException.scala
// Since: 2012/11/19 2:08 PM
//
//--------------------------------------

object SilkException {

  def error(m:String) = {
    val t = new Throwable
    val caller = t.getStackTrace()(2)

    throw new SilkExceptionBase(s"$m in ${caller.getMethodName}(${caller.getFileName}:${caller.getLineNumber})") {
    }
  }
  def error(e:Throwable) = {
    val caller = e.getStackTrace()(2)
    throw new SilkExceptionBase(s"${e.getMessage} in ${caller.getMethodName}(${caller.getFileName}:${caller.getLineNumber})") {}
  }

  def pending : Nothing = {
    val t = new Throwable
    val caller = t.getStackTrace()(1)
    throw Pending(caller.getMethodName)
  }

  def NA : Nothing = {
    val t = new Throwable
    val caller = t.getStackTrace()(1)
    throw NotAvailable(s"${caller.getMethodName} (${caller.getFileName}:${caller.getLineNumber})")
  }

}


trait SilkException {

  def errorCode = CName.toNaturalName(this.getClass.getSimpleName).toUpperCase

  override def toString = {
    "[%s] %s".format(errorCode, super.toString)
  }
}


/**
 * @author leo
 */
abstract class SilkExceptionBase(private val message:String) extends Exception(message) with SilkException {
}

abstract class SilkError(private val message:String) extends Error(message) with SilkException {
}

case class Pending(method:String) extends SilkExceptionBase(s"the implementation of $method")
case class NotAvailable(method:String) extends SilkExceptionBase(s"the implementation of $method")
