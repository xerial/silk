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



//--------------------------------------
//
// UnionFind.scala
// Since: 2012/04/05 15:03
//
//--------------------------------------

/**
 * Union-find based disjoint set implementation
 *
 * @author leo
 *
 */
class UnionFindSet[E] extends collection.mutable.Set[E] {

  /**
   * Holder of the element with its rank and the parent node
   * @param elem
   * @param parent
   * @param rank
   */
  private class Container(val elem: E, var parent: E, var rank: Int) {
    def isRoot : Boolean = elem eq parent
  }

  private val elemToContainerIndex = collection.mutable.Map[E, Container]()


  /**
   * Retrieve the container of the element e
   * @param e
   * @return container of e
   */
  private def containerOf(e: E): Container = {
    def newContainer = new Container(e, e, 0) // Set the parent to this element

    // If no container for e is found, create a new one
    elemToContainerIndex.getOrElseUpdate(e, newContainer)
  }

  /**
   * Add a new element
   * @param e
   */
  def +=(e: E): this.type = {
    containerOf(e) // create a new containerOf for e if it does not exist
    this
  }

  def -=(e: E): this.type = {
    throw new UnsupportedOperationException("removal")
  }

  /**
   * Find the representative element of the class to which e belongs
   * @param e
   * @return
   */
  def find(e: E) : E = {
    val c = containerOf(e)
    if(c.isRoot)
      e
    else {
      // path compression: the parent of e
      c.parent = find(c.parent)
      c.parent
    }
  }

  /**
   * Union the two sets containing x and y
   * @param x
   * @param y
   */
  def union(x: E, y: E) {

    val xRoot = containerOf(find(x))
    val yRoot = containerOf(find(y))

    // Compare the rank of two root nodes
    if (xRoot.rank > yRoot.rank) {
      // x has a higher rank
      yRoot.parent = xRoot.elem
    }
    else {
      // y has a higher rank
      xRoot.parent = yRoot.elem
      // If the ranks are the same, increase the rank of the others
      if (xRoot.rank == yRoot.rank)
        yRoot.rank += 1
    }
  }

  private def containerList = elemToContainerIndex.values

  override def size = elemToContainerIndex.size

  def contains(e: E) = elemToContainerIndex.contains(e)

  /**
   * Iterator of the elements contained in this set
   * @return
   */
  def iterator = containerList.map(_.elem).toIterator

  /**
   * Iterator of the root nodes of the groups
   * @return
   */
  def representatives: Iterable[E] =
    for(c <- containerList; c.isRoot) yield c.elem


  /**
   * Return the elements belonging to the same group with e
   * @param e
   * @return
   */
  def elementsInTheSameClass(e: E) : Iterable[E] = {
    val root = containerOf(find(e))
    for(c <- containerList; if find(c.elem) == root.elem) yield c.elem
  }

  /**
   * Iterator of each group
   * @return
   */
  def groups: Iterable[Iterable[E]] =
    for (r <- representatives) yield elementsInTheSameClass(r)


}
