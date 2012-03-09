package xerial.silk.util

import collection.immutable.Map.Map1
import collection.JavaConversions.JConcurrentMapWrapper
import collection.mutable.{ConcurrentMap, MapLike}
import java.util.concurrent.ConcurrentSkipListMap

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

//--------------------------------------
//
// LSMTree.scala
// Since: 2012/02/19 3:41
//
//--------------------------------------

object LSMTree {

  private object EmptyMap extends Map[Any, Nothing] with Serializable {
    override def size: Int = 0

    def get(key: Any): Option[Nothing] = None

    def iterator: Iterator[(Any, Nothing)] = Iterator.empty

    override def updated[B1](key: Any, value: B1): Map[Any, B1] = new Map1(key, value)

    def +[B1](kv: (Any, B1)): Map[Any, B1] = updated(kv._1, kv._2)

    def -(key: Any): Map[Any, Nothing] = this
  }

}

/**
 * @author leo
 */
class LSMTree[A, B] extends ConcurrentSkipListMap[A, B] {
//  private val cache = new JConcurrentMapWrapper(new ConcurrentSkipListMap[A, B])
//
//  def empty = cache.empty.asInstanceOf[LSMTree[A, B]]
//
//  def get(key: A) = cache.get(key)
//
//
//  def iterator = cache.iterator
//
//  def seq = cache.seq
//
//  def +=(kv: (A, B)) = cache.+=(kv).asInstanceOf[this.type]
//
//  def -=(key: A) = cache.-=(key).asInstanceOf[this.type]
}