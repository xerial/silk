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

package xerial.silk.glens

import collection.mutable.{Stack, ArrayBuilder}

//--------------------------------------
//
// PrioritySearchTree.scala
// Since: 2012/04/02 1:36
//
//--------------------------------------


object PrioritySearchTree {
  abstract trait Visitor[E] {
    def visit(visit: E): Unit
  }
  abstract trait ResultHandler[E] {
    def handle(elem: E): Unit
    def toContinue: Boolean
  }

  class Node[E](var elem: E, var x: Int, var y: Int) {
    if (elem == null) throw new NullPointerException("node cannot be null")

    def swap(n: Node[E]): Unit = {
      val tmpX: Int = this.x
      this.x = n.x
      n.x = tmpX
      val tmpY: Int = this.y
      this.y = n.y
      n.y = tmpY
      val tmpNode: E = this.elem
      this.elem = n.elem
      n.elem = tmpNode
    }
    def replaceWith(n: Node[E]): Unit = {
      this.x = n.x
      this.y = n.y
      this.elem = n.elem
    }
    var splitX: Int = 0
    var left: Node[E] = null
    var right: Node[E] = null
  }

  private[PrioritySearchTree] class QueryBox(val x1: Int, val x2: Int, val upperY: Int = 0)
  private[PrioritySearchTree] class QueryContext[E](val current: E, val x1: Int, val x2: Int)

  private class ResultCollector[E] extends ResultHandler[E] {
    private val b = Seq.newBuilder[E]
    def handle(elem: E): Unit = {
      b += elem
    }
    def toContinue: Boolean = {
      return true
    }
    def result = b.result
  }
}
/**
 * Priority search tree for efficient 2D search
 *
 * @author leo
 *
 */
class PrioritySearchTree[E](lowerBoundOfX: Int = 0, upperBoundOfX: Int = Int.MaxValue, lowerBoundOfY: Int = 0, upperBoundOfY: Int = Int.MaxValue)
  extends Iterable[E] {

  import PrioritySearchTree._

  type Node = PrioritySearchTree.Node[E]

  private var root: Node = null
  private var nodeCount: Int = 0


  def clear: Unit = {
    root = null
    nodeCount = 0
  }

  override def size: Int = {
    return nodeCount
  }
  /**
   * Retrieves elements contained in the specified range, (X:[x1, x2], Y:[ , upperY]). This query is useful for
   * answering the interval intersection problem.
   *
   * @param x1
   * @param x2
   * @param upperY
   * @return elements contained in the range (X:[x1, x2], Y:[ , upperY])
   */
  def rangeQuery(x1: Int, x2: Int, upperY: Int) : Seq[E] = {
    val resultCollector: PrioritySearchTree.ResultCollector[E] = new PrioritySearchTree.ResultCollector[E]
    rangeQuery_internal(root, new QueryBox(x1, x2, upperY), x1, x2, resultCollector)
    resultCollector.result
  }

  def rangeQuery(x1: Int, x2: Int, upperY: Int, handler: PrioritySearchTree.ResultHandler[E]): Unit = {
    rangeQuery_internal(root, new PrioritySearchTree.QueryBox(x1, x2, upperY), x1, x2, handler)
  }

  private def rangeQuery_internal(currentNode: Node, queryBox:QueryBox, rangeX1: Int, rangeX2: Int, resultHandler: PrioritySearchTree.ResultHandler[E]): Boolean = {
    var toContinue: Boolean = resultHandler.toContinue
    if (!toContinue || rangeX1 > rangeX2) return false
    if (currentNode == null) return toContinue
    val contextStack: Stack[QueryContext[Node]] = new Stack[QueryContext[Node]]
    contextStack.push(new QueryContext[Node](currentNode, rangeX1, rangeX2))
    while (toContinue && !contextStack.isEmpty) {
      val context: QueryContext[Node] = contextStack.pop()
      if (context.current.y <= queryBox.upperY) {
        if (queryBox.x1 <= context.current.x && context.current.x <= queryBox.x2) {
          resultHandler.handle(context.current.elem)
          toContinue = resultHandler.toContinue
        }
        val middleX: Int = context.current.splitX
        if (toContinue) {
          if (context.current.right != null && middleX <= queryBox.x2) {
            contextStack.push(new QueryContext[Node](context.current.right, middleX, context.x2))
          }
          if (context.current.left != null && queryBox.x1 < middleX) {
            contextStack.push(new QueryContext[Node](context.current.left, context.x1, middleX))
          }
        }
      }
    }
    return toContinue
  }
  /**
   * Insert a new node
   *
   * @param elem
   */
  def insert(elem: E, x: Int, y: Int): Unit = {
    root = insert_internal(root, new Node(elem, x, y), lowerBoundOfX, upperBoundOfX)
  }

  /**
   * Remove the specified node
   *
   * @param elem
   * @return true if the specified element exists in the tree
   */
  def remove(elem: E, x: Int, y: Int): Boolean = {
    val prevNumNodes: Int = size
    root = remove_internal(root, new Node(elem, x, y), lowerBoundOfX, upperBoundOfX)
    return prevNumNodes != size
  }
  private def insert_internal(currentNode: Node, insertNode: Node, lowerRangeOfX: Int, upperRangeOfX: Int): Node = {
    val newNode = if (currentNode == null) {
      insertNode.splitX = (lowerRangeOfX + upperRangeOfX) / 2
      nodeCount += 1;
      insertNode
    }
    else {
      if (insertNode.y < currentNode.y) {
        currentNode.swap(insertNode)
      }
      if (insertNode.x < currentNode.splitX) currentNode.left = insert_internal(currentNode.left, insertNode, lowerRangeOfX, currentNode.splitX)
      else currentNode.right = insert_internal(currentNode.right, insertNode, currentNode.splitX, upperRangeOfX)
      currentNode
    }
    return newNode
  }

  private def remove_internal(currentNode: Node, removeTarget: Node, x_lower: Int, x_upper: Int): Node = {
    if (currentNode == null) {
      return currentNode
    }
    if (currentNode.elem == removeTarget.elem) {
      if (currentNode.left != null) {
        if (currentNode.right != null) {
          if (currentNode.left.y < currentNode.right.y) {
            currentNode.replaceWith(currentNode.left)
            currentNode.left = remove_internal(currentNode.left, currentNode.left, x_lower, x_upper)
          }
          else {
            currentNode.replaceWith(currentNode.right)
            currentNode.right = remove_internal(currentNode.right, currentNode.right, x_lower, x_upper)
          }
        }
        else {
          currentNode.replaceWith(currentNode.left)
          currentNode.left = remove_internal(currentNode.left, currentNode.left, x_lower, x_upper)
        }
      }
      else {
        if (currentNode.right != null) {
          currentNode.replaceWith(currentNode.right)
          currentNode.right = remove_internal(currentNode.right, currentNode.right, x_lower, x_upper)
        }
        else {
          ({
            nodeCount -= 1;
            nodeCount
          })
          return null
        }
      }
    }
    else {
      if (removeTarget.x < currentNode.splitX) currentNode.left = remove_internal(currentNode.left, removeTarget, x_lower, currentNode.splitX)
      else currentNode.right = remove_internal(currentNode.right, removeTarget, currentNode.splitX, x_upper)
    }
    return currentNode
  }
  def iterator: Iterator[E] = {
    return new DFSIterator(root)
  }
  def depthFirstSearch(visitor: PrioritySearchTree.Visitor[E]): Unit = {
    dfs(root, visitor)
  }
  private def dfs(current: Node, visitor: PrioritySearchTree.Visitor[E]): Unit = {
    if (current == null) return
    visitor.visit(current.elem)
    dfs(current.left, visitor)
    dfs(current.right, visitor)
  }
  private class DFSIterator extends Iterator[E] {
    private val nodeStack: Stack[Node] = new Stack[Node]

    def this(root: Node) {
      this()
      if (root != null) nodeStack.push(root)
    }
    def hasNext: Boolean = {
      return !nodeStack.isEmpty
    }
    def next: E = {
      if (!hasNext) return null.asInstanceOf[E]
      val nextNode: Node = nodeStack.pop
      if (nextNode.right != null) nodeStack.push(nextNode.right)
      if (nextNode.left != null) nodeStack.push(nextNode.left)
      return nextNode.elem
    }
    def remove: Unit = {
      throw new UnsupportedOperationException("remove")
    }
  }
}
