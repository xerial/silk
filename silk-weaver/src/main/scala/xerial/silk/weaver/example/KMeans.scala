//--------------------------------------
//
// KMeans.scala
// Since: 2012/12/13 11:34 AM
//
//--------------------------------------

package xerial.silk.example


import xerial.silk._
import annotation.tailrec
import xerial.core.log.Logger
import scala.util.Random
import scala.reflect.ClassTag

/**
 * @author Taro L. Saito
 */
object KMeans extends Logger {

  import Silk._

  implicit val silk = Silk.init()

  def main(args:Array[String]) {

    val N = 1000
    val D = 2
    val points = for(i <- 0 until N) yield {
      val x = (0 until D).map{ dim => Random.nextDouble }.toArray
      P("P%d".format(i+1), new Point(x))
    }

    val k = 5
    kMeans(k, points.toSilk, (0 until k).map { i => points(Random.nextInt(N)).x }.toArray)(scala.reflect.classTag[P], MyDist)
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


  class Cluster[A](val point: SilkSeq[A], val centroid: Array[Point], val clusterAssignment: SilkSeq[Int])(implicit ev:ClassTag[A], m: PointType[A]) {
    val K = centroid.size
    val N = point.size.get

    def clusterSet: Map[Int, SilkSeq[A]] = ((0 until K).map{ cid => cid -> pointsInCluster(cid)}).toMap[Int, SilkSeq[A]]

    def pointsInCluster(cid: Int): SilkSeq[A] = {
      for((p, c) <- point.zip(clusterAssignment); if c == cid) yield p
    }

    def centerOfMass : Array[Point] = {
      val cluster : SilkSeq[SilkSeq[A]] = (0 until K).toSilk.map{ pointsInCluster }
      val r = cluster.map { points =>
        val sum = points.map{ m.toPoint(_) }.reduce[Point]{case (p1, p2) => p1 + p2 }
        val cN = points.size.get.toInt
        sum.get / cN
      }
      r.toArray
    }

    def hasDuplicateCentroids = {
      centroid.distinct.size != centroid.size
    }

    lazy val averageOfSquaredDistances = {
      val dist = point.zip(clusterAssignment) map {
        case (a, cid) => m.toPoint(a).distance(centroid(cid))
      }
      dist.sum[Double].get / N
    }

    override def toString = clusterSet.map {
      case (k, v) => "centroid:%,d [%s]".format(centroid(k), v.mkString(","))
    }.mkString("\n")
  }


  def kMeans[A](K: Int, point: SilkSeq[A], initialCentroid: Array[Point], maxIteration:Int=300)(implicit ev:ClassTag[A], m : PointType[A]): Cluster[A] = {
    require(K == initialCentroid.size, "K and centroid size must be equal")
    // Assign each point to the closest centroid
    def EStep(c: Cluster[A]): Cluster[A] = {
      val assignment = point.map { p => (0 until c.K).minBy{ cid => m.toPoint(p).distance(c.centroid(cid))} }
      new Cluster(c.point, c.centroid, assignment)
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


