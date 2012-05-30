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

package xerial.silk.model

import collection.GenTraversableOnce
import collection.generic.{CanBuildFrom, FilterMonadic}

//--------------------------------------
//
// SilkIO.scala
// Since: 2012/03/13 14:41
//
//--------------------------------------
case class SilkContext(name:String, context:Any)
case class SilkParticle(context:SilkContext, elem:AnyRef)

/**
 * Pull-style Silk data input channel
 */
trait SilkInputChannel {
  def hasNext : Boolean
  def next : SilkParticle
  def close : Unit
}

/**
 * Pull-style Silk data output channel
 */
trait SilkOutputChannel {
  def write[A](obj:A) : Unit
  def context[A](name:String, context:A)(body:SilkOutputChannel => Unit)
  def close : Unit
}

trait SilkInput[+Repr] extends TraversableOnce[SilkParticle] {
  def schema : SilkSchema
  def close : Unit

  def foreach[U](f: SilkParticle => U) : Unit

//  def selectAll[A](implicit m:Manifest[A]) : FilterMonadic[A, Repr] = {
//
//  }
//
//  def select[A](condition: A => Boolean)(implicit m:Manifest[A]) = {
//
//
//  }
//
//  class StreamWithFilter[A] extends FilterMonadic[A, Repr] {
//    def map[B, That](f: (A) => B)(implicit bf: CanBuildFrom[Repr, B, That]) = {
//      null
//    }
//
//    def flatMap[B, That](f: (A) => GenTraversableOnce[B])(implicit bf: CanBuildFrom[Repr, B, That]) = null
//
//    def foreach[U](f: (A) => U) {}
//
//    def withFilter(p: (A) => Boolean) = null
//  }

}

//class SilkFileInput(fileName:String) extends SilkInput[SilkFileInput] {
//  def schema = null
//  def inputChannel = null
//  def close {}
//}
//
//object SilkIO {
//  def readFile(fileName:String)(body: SilkInput => Unit) = {
//    val source = new SilkFileInput(fileName)
//    try {
//      body(source)
//    }
//    finally {
//      source.close
//    }
//  }
//
//
//}