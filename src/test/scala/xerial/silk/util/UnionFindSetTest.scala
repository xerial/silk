//--------------------------------------
//
// UnionFindSetTest.scala
// Since: 2012/07/03 11:07 AM
//
//--------------------------------------

package xerial.silk.util

import util.Random

/**
 * @author leo
 */
class UnionFindSetTest extends SilkSpec {


  class Maze(width: Int) {

    // left walls of each cell
    private val right_wall = Array.ofDim[Boolean](width, width)
    // bottom walls of each cell
    private val bottom_wall = Array.ofDim[Boolean](width, width)

    for (x <- 0 until width; y <- 0 until width) {
      right_wall(x)(y) = true
      bottom_wall(x)(y) = true
    }

    override def toString = {
      val s = new StringBuilder
      // top wall
      (0 until width).foreach(i => s.append("+-"))
      s.append("+\n")
      for (y <- 0 until width) {
        s.append(if (y == 0) " " else "|") // left most wall
        for (x <- 0 until width) {
          if (x == width - 1 && y == width - 1)
            s.append("  ")
          else
            s.append(if (right_wall(x)(y)) " |" else "  ")
        }
        s.append("\n")

        s.append("+")
        for (x <- 0 until width) {
          s.append(if (bottom_wall(x)(y)) "-+" else " +")
        }
        s.append("\n")
      }
      s.toString
    }

    def numberOfWalls(x:Int, y:Int) : Int = {
      var count = 0
      if(bottom_wall(x)(y))
        count +=1
      if(y == 0 || bottom_wall(x)(y-1))
        count += 1
      if(x == 0 || right_wall(x-1)(y))
        count += 1
      if(right_wall(x)(y))
        count += 1
      return count
    }


    def breakRightWall(x: Int, y: Int): Boolean = {
      if (x >= 0 && x < width - 1) {
        right_wall(x)(y) = false
        true
      }
      else
        false
    }

    def breakBottomWall(x: Int, y: Int): Boolean = {
      if (y >= 0 && y < width - 1) {
        bottom_wall(x)(y) = false
        true
      }
      else
        false
    }
  }


  "UnionFindSet" should {

    "create a maze" in {
      val w = 10
      val m = new Maze(w)

      info("Randomly break walls")
      val r = new Random(14)
      val uf = new UnionFindSet[Int]

      def cellID(x: Int, y: Int) = x + y * w

      def randomBreak: Unit = {
        val x = r.nextInt(w)
        val y = r.nextInt(w)
        val rightOrBottom = r.nextBoolean
        val a = cellID(x, y)
        val b = cellID(if (rightOrBottom) x + 1 else x, if (rightOrBottom) y else y + 1)
        if(uf.find(a) != uf.find(b)) {
          val broken = {
            if (rightOrBottom)
              m.breakRightWall(x, y)
            else
              m.breakBottomWall(x, y)
          }

          if (broken) {
            uf.union(a, b)
          }
        }
      }

      def hasPass = uf.find(cellID(0, 0)) == uf.find(cellID(w - 1, w - 1))

      while (!hasPass) {
        randomBreak
      }
      info(m)

    }


  }

}