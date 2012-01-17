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

package xerial.silk.util

import scala.Null
import collection.mutable.WeakHashMap
import java.lang.ref.WeakReference
import collection.MapLike

//--------------------------------------
//
// Cache.scala
// Since: 2012/01/17 15:55
//
//--------------------------------------

/**
 * Cache for unique values associated to keys.
 * For each unique key, only one instance of the value will be generated.
 *
 * Entries are removed from this cache When the key is no longer referenced.
 *
 * @author leo
 */
class Cache[K, V >: Null](factory: K => V) extends Map[K, V] with MapLike[K, V, Cache[K, V]] {
  private val cache = new WeakHashMap[K, WeakReference[V]]

  override def empty = new Cache[K, V]( _ => null.asInstanceOf[V] )

  override def apply(key: K): V = {
    def findFromCache: V = {
      if (cache.contains(key))
        cache(key).get
      else
        null.asInstanceOf[V]
    }
    def createEntry: V = {
      val newValue = factory(key)
      cache.put(key, new WeakReference(newValue))
      newValue
    }

    val v = findFromCache
    if (v == null)
      createEntry
    else
      v
  }

  def get(key: K) = {
    if (cache.contains(key))
      Some(cache(key).get)
    else
      None
  }

  def iterator = new Iterator[(K, V)] {
    val it = cache.iterator

    def hasNext = it.hasNext

    def next = {
      val e = it.next()
      (e._1, e._2.get)
    }
  }


  def -(key: K) = null

  def +[B1 >: V](kv: (K, B1)) = null
}