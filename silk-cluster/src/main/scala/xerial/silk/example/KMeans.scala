//--------------------------------------
//
// KMeans.scala
// Since: 2012/12/13 11:34 AM
//
//--------------------------------------

package xerial.silk.example


import xerial.silk._
import annotation.tailrec
import core.Silk
import xerial.core.log.Logger
import scala.util.Random

/**
 * @author Taro L. Saito
 */
object KMeans extends Logger {

  def main(args:Array[String]) {

    val N = 1000
    val D = 2
    val points = for(i <- 0 until N) yield {
      val x = (0 until D).map{ dim => Random.nextDouble }.toArray
      P("P%d".format(i+1), new Point(x))
    }

    val k = 5
    kMeans(k, points.toSilk, (0 until k).map { i => points(Random.nextInt(N)).x }.toArray)(MyDist)
  }

  trait PointType[A] {
    def toPoint(a:A) : Point

  }

  case class P(name:String, x:Point) {

  }

  object MyDist extends PointType[P] {
    def toPoint(a:P) = a.x
  }


  class Point(val x:Array[Double]) extends Iterable[Double] {
    def iterator = x.iterator
    def +(other:Point) = new Point(x.zip(other.x).map { case (a, b) => a + b })
    def -(other:Point) = new Point(x.zip(other.x).map { case (a, b) => a - b })
    def /(n:Int) = new Point(x.map { _ / n })
    def squaredDistance(other:Point) : Double = ((this - other).map { e => math.pow(e, 2.0) }).sum
    def distance(other:Point) : Double = math.sqrt(squaredDistance(other))
  }


  class Cluster[A](val point: Silk[A], private val _centroid: Array[Point], val clusterAssignment: Silk[Int])(implicit m: PointType[A]) {
    val K = _centroid.size
    val N = point.size

    def clusterSet: Map[Int, Silk[A]] = ((0 until K).map{ cid => cid -> pointsInCluster(cid)}).toMap[Int, Silk[A]]

//    def extractCluster(cid: Int): Cluster[A] = {
//      val pts = pointsInCluster(cid)
//      new Cluster(pts, Array(centroid(cid)), pts.map { p => 0 })
//    }

    def pointsInCluster(cid: Int): Silk[A] = {
      for((p, c) <- point.zip(clusterAssignment); if c == cid) yield p
    }

    def centerOfMass : Array[Point] = {
      val cluster : Seq[Silk[A]] = (0 until K).map{ pointsInCluster }
      cluster.map { points =>
        val sum : Point = points.map{ m.toPoint(_) }.reduce[Point]{case (p1, p2) => p1 + p2 }.get
        sum / points.size.toInt
      }.toArray
    }

    def centroids: Array[Point] = _centroid

    def centroid(cid: Int): Point = centroid(cid)

    def hasDuplicateCentroids = {
      centroids.distinct.size != centroids.size
    }

//    def sumOfSquaredError(cid: Int): Double = {
//      val dist = pointsInCluster(cid) map {
//        p => m.toPoint(p).distance(centroid(cid))
//      }
//      dist.sum.get
//    }

    lazy val averageOfSquaredDistances: Double = {
      val dist = point.zip(clusterAssignment) map {
        case (a, cid) => m.toPoint(a).distance(centroid(cid))
      }
      dist.sum.get / point.size
    }

    override def toString = clusterSet.map {
      case (k, v) => "centroid:%,d [%s]".format(centroid(k), v.mkString(","))
    }.mkString("\n")
  }


  def kMeans[A](K: Int, point: Silk[A], initialCentroid: Array[Point], maxIteration:Int=300)(implicit m : PointType[A]): Cluster[A] = {
    require(K == initialCentroid.size, "K and centroid size must be equal")
    // Assign each point to the closest centroid
    def EStep(c: Cluster[A]): Cluster[A] = {
      val assignment = point.map { p => (0 until c.K).minBy{ cid => m.toPoint(p).distance(c.centroid(cid))} }
      new Cluster(c.point, c.centroids, assignment)
    }

    // Fix centroid positions by taking the center of mass of the data points
    def MStep(c: Cluster[A]): Cluster[A] = {
      new Cluster(c.point, c.centerOfMass, c.clusterAssignment)
    }

    @tailrec
    def iteration(i: Int, current: Cluster[A]): Cluster[A] = {
      if (i >= maxIteration)
        current
      else {
        trace(f"K-means: iteration $i%d\n$current%s")
        val newCluster = EStep(current)
        if (newCluster.averageOfSquaredDistances >= current.averageOfSquaredDistances)
          current
        else {
          val refined = MStep(newCluster)
          if (refined.hasDuplicateCentroids)
            current
          else
            iteration(i + 1, refined)
        }
      }
    }

    iteration(0, new Cluster(point, initialCentroid, point.map { p => 0 }))
  }

}


