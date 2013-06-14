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

package xerial.silk

import xerial.core.util.CName
import scala.reflect.macros.Context

//--------------------------------------
//
// SilkException.scala
// Since: 2012/11/19 2:08 PM
//
//--------------------------------------

object SilkException {


  def pending : Pending = {
    val t = new Throwable
    val caller = t.getStackTrace()(2)
    Pending(caller.getMethodName)
  }

  def na : NotAvailable = {
    val t = new Throwable
    val caller = t.getStackTrace()(2)
    NotAvailable(caller.getMethodName)
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

case class Pending(method:String) extends SilkExceptionBase(s"pending implementation of $method")
case class NotAvailable(method:String) extends SilkExceptionBase(s"Not available implementation of $method")
case class TimeOut(message:String) extends SilkExceptionBase(s"Time out: $message")

case class ConnectionLoss() extends SilkExceptionBase(s"connection loss")

case class InvalidFormat(message:String) extends SilkExceptionBase(message)


case class ParseError(line:Int, pos:Int, message:String)
  extends SilkExceptionBase("(line:%d, pos:%d) %s".format(line, pos, message))

case object ZookeeperClientIsClosed extends SilkExceptionBase("")
case object EmptyConnectionException extends SilkExceptionBase("")