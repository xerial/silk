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

import collection.mutable.{HashSet, ArrayBuffer}


//--------------------------------------
//
// UnionFind.scala
// Since: 2012/04/05 15:03
//
//--------------------------------------

/**
 * Union-Find disjoint set implementation
 *
 * @author leo
 *
 */
class DisjointSet[E] extends collection.mutable.Set[E] {

  private class ElementTable {
    val elemToID = collection.mutable.Map[E, Int]()
    val idToElem = collection.mutable.Map[Int, E]()
    private var counter = 0

    /**
     * Add element to the table
     * @param e 
     * @return true if the element is newly added, otherwise false
     */
    def add(e: E): Boolean = {
      if (elemToID.contains(e))
        false
      else {
        issueNewID(e)
        true
      }
    }

    private def issueNewID(e: E) = {
      val newID = counter
      counter += 1
      idToElem += newID -> e
      newID
    }

    def idOf(e: E): Int = elemToID.getOrElseUpdate(e, issueNewID(e))
    def elementOf(id: Int): E = idToElem(id)
  }

  private val elementIndex = new ElementTable
  private val parentID = new ArrayBuffer[Int]
  private val rank = new ArrayBuffer[Int]

  def id(e: E) = elementIndex.idOf(e)
  def element(id: Int) = elementIndex.elementOf(id)

  /**
   * @param element
   */
  def +=(element: E) : this.type = {
    val isNewElement = elementIndex.add(element);
    if (isNewElement) {
      val id = id(element);
      parentID += id
      rank += 0
    }
    this
  }
  
  def -=(element:E) : this.type = {
    throw new UnsupportedOperationException("removal")
  }
  def contains(e:E) = {
    elementIndex.elemToID.contains(e)
  }

  def iterator = elementIndex.elemToID.keysIterator

  def groups : Iterable[Seq[E]] = {
    for(r <- representativeElements) yield {
      elementsInTheSameClass(r).toSeq
    }
  }

  /**
   * Find the disjoint set ID of the given element
   *
   * @param e
   *            element
   * @return
   */
  def findClassID(e: E) = classID(id(e))

  private def classID(id:Int) : Int = {
    if (id != parentID(id)) {
      // path compression
      parentID(id) = classID(parentID(id))
    }
    parentID(id)
  }
  
  def representativeElements: Iterable[E] = {
    for ((id, elem) <- elementIndex.idToElem if id == classID(id))
    yield elem
  }

  def elementsInTheSameClass(e: E) = {
    val classID = findClassID(e)
    for (i <- 0 until parentID.length; if parentID(0) == classID) yield {
      element(i)
    }
  }

  def union(x: E, y: E) {
    linkByID(findClassID(x), findClassID(y))
  }

  def link(x: E, y: E) {
    val xID = id(x)
    val yID = id(y)
    linkByID(xID, yID);
  }

  private def linkByID (xID:Int, yID:Int) {
    if (rank(xID) > rank(yID)) {
      parentID(yID) = xID
    }
    else {
      parentID(xID) = yID
      if (rank(xID) == rank(yID))
        rank(yID) = rank(yID) + 1
    }
  }

  override def size = {
    elementIndex.elemToID.size
  }
  
  

}
