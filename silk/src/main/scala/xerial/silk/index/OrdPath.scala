//--------------------------------------
//
// OrdPath.scala
// Since: 2013/01/17 9:47 AM
//
//--------------------------------------

package xerial.silk.index

import xerial.core.log.Logger

/**
 * OrdPath is a value of dot-separated integers that represents a position in a tree
 *
 * @author Taro L. Saito
 */
class OrdPath(path:Array[Int]) {

  override def toString = path.mkString(".")
  def apply(index:Int) : Int = path(index)

  def length : Int = path.length

  def parent : Option[OrdPath] =
    if(path.length > 1)
      Some(new OrdPath(path.slice(0, path.length-1)))
    else
      None

  def :+(childIndex:Int) = new OrdPath(path :+ childIndex)

  /**
   * Get a next OrdPath that incremented the value at the specified level by 1.
   * When a higher (left) value is incremented, the remaining (right) values will be cleared.
   * For example, if `1.2.4` is incremented at level 0, it becomes `2`.
   * When 1.2.4 is incremented at level 1, it becomes `1.3`.
   * When the increment level is higher than the length of a OrdPath, 0 will be padded until the specified level.
   * The next(2) of OrdPath `1.2` will be `1.2.1`.
   * The next(3) of OrdPath `1.2`. will be `1.2.0.1`.
   *
   *@param level
   */
  def next(level:Int) : OrdPath = {
    val newPath = Array.ofDim[Int](level+1)
    var i = 0
    while(i < newPath.length) {
      if(i < path.length)
        newPath(i) = path(i)
      else
        newPath(i) = 0
      i += 1
    }
    newPath(level) += 1
    new OrdPath(newPath)
  }

  private var hash = 0

  override def hashCode = {
    if(hash == 0) {
      var h = 0
      var i = 0
      while(i < path.length) {
        h *= 31
        h += path(i)
        i += 1
      }
      hash = h
    }
    hash
  }

  override def equals(other:Any) : Boolean = {
    if(!other.isInstanceOf[OrdPath])
      return false


    val o = other.asInstanceOf[OrdPath]
    if(this.length != o.length)
      return false
    var i = 0
    while(i < path.length) {
      if(this(i) != o(i))
        return false
      i += 1
    }
    true

  }

}

object OrdPath extends Logger {

  val zero = OrdPath("0")
  val one = OrdPath("1")

  /**
   * Create a new OrdPath from a string
   * @param s
   * @return
   */
  def apply(s:String) : OrdPath = {
    val c = s.split("\\.")
    new OrdPath(c.map( _.toInt ))
  }

  def unapply(s:String) : Option[OrdPath] = {
    try
      Some(apply(s))
    catch {
      case e => None
    }
  }

}