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

package xerial.silk.util.mining

import java.util.Random
import xerial.silk.util.Logging
import collection.JavaConversions._
import collection.{GenTraversable, GenTraversableOnce, GenSeq}

//--------------------------------------
//
// KMeans.scala
// Since: 2012/03/15 22:15
//
//--------------------------------------

/**
 * Distance definition of data points for K-Means clustering
 *
 * @author leo
 *
 */
trait PointDistance[T] {

  //  /**
  //   * Get point vector of the element
  //   * @param e
  //   * @return
  //   */
  def getPoint(e: T): Array[Double]

  /**
   * Return the distance between the given two points
   *
   * @param a
   * @param b
   * @return |a-b|
   */
  def distance(a: Array[Double], b: Array[Double]): Double = {
    val m = a.size
    val sum = (0 until m).par.map(col => Math.pow(a(col) - b(col), 2)).sum
    Math.sqrt(sum)
  }

  /**
   * Return the center of mass of the inputs
   *
   * @param points
   *            list of input points
   * @return |a+b+c+... | / N
   */
  def centerOfMass(points: GenTraversableOnce[Array[Double]]): Array[Double] = {
    var n = 0
    val v = points.reduce((a, b) => {
      n += 1;
      a.zip(b).map(x => x._1 + x._2)
    })
    v.map(x => x / n)
  }

  def squareError(points: GenTraversableOnce[Array[Double]], centor: Array[Double]): Double = {
    points.reduce((a, b) => a.zip(b).map(x => Math.pow(x._1 - x._2, 2))).sum
  }

  /**
   * Compute the lower bound of the points
   *
   * @param points
   * @return
   */
  def lowerBound(points: GenTraversableOnce[Array[Double]]): Array[Double] = {
    points.reduce((a, b) => a.zip(b).map(x => Math.min(x._1, x._2)))
  }

  /**
   * Compute the upper bound of the points
   *
   * @param points
   * @return
   */
  def upperBound(points: GenTraversableOnce[Array[Double]]): Array[Double] = {
    points.reduce((a, b) => a.zip(b).map(x => Math.max(x._1, x._2)))
  }
  /**
   * Move the points to the specified direction to the amount of the given distance
   *
   * @param point
   * @return
   */
  def move(point: Array[Double], direction: Array[Double]): Array[Double] = {
    point.zip(direction).map(x => x._1 + x._2)
  }
  /**
   * The size of dimension;
   *
   * @return
   */
  def dimSize: Int
}

object KMeans {

  class Config {
    var maxIteration: Int = 300
  }

  /**
   * Holds K-means clustering result
   *
   * @author leo
   *
   */
  case class ClusterSet[T](K: Int, N: Int, points: Array[T], metric: PointDistance[T], centroids: Array[Array[Double]], clusterAssignment: Array[Int]) {

    lazy val averageOfDistance: Double = {
      // TODO map(point -> distance) => reduce(seq[distance] -> sum(distance)
      val dist = clusterAssignment.par.zipWithIndex.map {
        case (clusterID, pointIndex) =>
          val centroid = centroids(clusterID)
          val point = points(pointIndex)
          Math.pow(metric.distance(centroid, metric.getPoint(point)), 2)
      }
      val distSum = dist.par.reduce((a, b) => a + b)
      distSum / points.length
    }

    /**
     * Average of squared distance of each point to its belonging centroids
     */
    //var averageOfDistance: Double = Double.MAX_VALUE
    /**
     * List of centroids
     */
    //var centroid: IndexedSet[T] = new IndexedSet[T]
    /**
     * Array of cluster IDs of the point p_0, ..., p_{N-1};
     */
  }

}

/**
 * K-means clustering
 *
 * @author leo
 *
 */
class KMeans[T](config: KMeans.Config, metric: PointDistance[T]) extends Logging {

  import KMeans._

  type Point = Array[Double]

  private val random: Random = new Random(0)

  implicit def toVector(point:T) : Point = metric.getPoint(point)

  private def hasDuplicate(points: GenTraversableOnce[T], other: T): Boolean = {
    points.find(p => metric.distance(p, other) == 0.0).isDefined
  }

  private def hasDuplicate(points: TraversableOnce[T]): Boolean = {
    val zero = Array.fill[Double](metric.dimSize)(0.0)
    val dist = (for (p <- points) yield metric.distance(p, zero)).toArray
    val dd = dist.distinct
    dist.size != dd.size
  }

  private def hasDuplicate(points:Seq[Point]) : Boolean = {

  }

  /**
   * Randomly choose K-centroids from the input data set
   *
   * @param K
   * @param points
   * @return
   */
  protected def initCentroids(K: Int, points: Array[T]): Seq[Point] = {
    val N: Int = points.size

    if (K > N)
      throw new IllegalArgumentException("K(%d) must be smaller than N(%d)".format(K, N))

    def pickCentroid(centroids: List[T], remaining: Int): List[T] = {
      val r = random.nextInt(N)
      if (!hasDuplicate(centroids, points(r)))
        pickCentroid(points(r) :: centroids, remaining - 1)
      else
        pickCentroid(centroids, remaining) // pick random node again
    }

    pickCentroid(List(), K).map(metric.getPoint(_))
  }

  /**
   * @param K
   *            number of clusters
   * @param points
   *            input data points in the matrix format
   * @throws Exception
   */
  def execute(K: Int, points: Array[T]): ClusterSet[T] = {
    execute(K, points, initCentroids(K, points))
  }
  /**
   * @param K
   *            number of clusters
   * @param points
   *            input data points in the matrix format
   * @param centroids
   *            initial centroids
   * @throws Exception
   */
  def execute(K: Int, points: Array[T], centroids: Seq[Array[Double]]): ClusterSet[T] = {
    val N: Int = points.length
    //var prevClusters: KMeans.ClusterSet[T] = new KMeans.ClusterSet[T](K, N, centroids)

    val maxIteration: Int = config.maxIteration
    def iteration(i: Int, cluster: ClusterSet[T]): ClusterSet[T] = {
      debug("iteration: %d", i + 1)
      if (i >= maxIteration)
        cluster
      else {
        val newCluster = EStep(K, points, centroids)
        if (newCluster.averageOfDistance >= cluster.averageOfDistance) {
          cluster
        }
        else {
          centroids = MStep(K, points, newClusters)
          if (hasDuplicate(centroids)) {
            return prevClusters
          }
          prevClusters = newClusters
        }
      }
    }


    var i: Int = 0
    while (i < maxIteration) {
      {
        if (_logger.isDebugEnabled) _logger.debug(String.format("iteration : %d", i + 1))
      }
      ({
        i += 1;
        i - 1
      })

    }
    return prevClusters
  }
  protected def EStep(K: Int, points: Array[T], centroids: Seq[Array[Double]]): ClusterSet[T] = {
    if (K != centroids.length)
      throw new IllegalStateException("K=%d, but # of centrods is %d".format(K, centroids.length))


    var result: KMeans.ClusterInfo[T] = new KMeans.ClusterInfo[T](K, N, centroids)
    var clusterAssignment: Array[Int] = result.clusterAssignment {
      {
        var i: Int = 0
        while (i < N) {
          {
            var p: T = points.get(i)
            var dist: Double = Double.MAX_VALUE
            var closestCentroidID: Int = -1 {
              var k: Int = 0
              while (k < K) {
                {
                  var d: Double = metric.distance(p, centroids.get(k))
                  if (d < dist) {
                    dist = d
                    closestCentroidID = k
                  }
                }
                ({
                  k += 1;
                  k - 1
                })
              }
            }
            assert((closestCentroidID != -1))
            clusterAssignment(i) = closestCentroidID
          }
          ({
            i += 1;
            i - 1
          })
        }
      }
    } {
      var averageOfDistance: Double = 0 {
        var i: Int = 0
        while (i < N) {
          {
            var p: T = points.get(i)
            var centroidOfP: T = result.centroid.getByID(clusterAssignment(i))
            averageOfDistance += Math.pow(metric.distance(p, centroidOfP), 2)
          }
          ({
            i += 1;
            i
          })
        }
      }
      averageOfDistance /= N
      result.averageOfDistance = averageOfDistance
    }
    return result
  }
  /**
   * Returns the list of new centroids
   *
   * @param K
   * @param points
   * @param clusterInfo
   * @return
   */
  protected def MStep(K: Int, points: List[T], clusterInfo: KMeans.ClusterInfo[T]): List[T] = {
    var newCentroid: List[T] = new ArrayList[T](K) {

      import scala.collection.JavaConversions._

      for (centroidID <- clusterInfo.centroid.getIDSet) {
        var centerOfMass: T = metric.centerOfMass(new KMeans#GroupElementsIterator(points, clusterInfo.clusterAssignment, centroidID))
        newCentroid.add(centerOfMass)
      }
    }
    return newCentroid
  }

  /**
   * Iterator for selecting points belonging to a specific centroid
   *
   * @author leo
   *
   */
  private class GroupElementsIterator extends Iterator[T] {
    def this(points: List[T], belongingClass: Array[Int], k: Int) {
      this()
      this.points = points
      this.belongingClass = belongingClass
      this.k = k
    }
    def hasNext: Boolean = {
      if (!queue.isEmpty) return true
      while (cursor < points.size) {
        {
          if (belongingClass(cursor) == k) {
            var next: T = points.get(cursor)
            queue.add(next)
            ({
              cursor += 1;
              cursor
            })
            return true
          }
        }
        ({
          cursor += 1;
          cursor - 1
        })
      }
      return false
    }
    def next: T = {
      if (hasNext) return queue.pollFirst
      else throw new NoSuchElementException
    }
    def remove: Unit = {
      throw new UnsupportedOperationException("remove")
    }
    private[tss] final val points: List[T] = null
    private[tss] final val belongingClass: Array[Int] = null
    private[tss] final val k: Int = 0
    private[tss] var queue: Deque[T] = new ArrayDeque[T]
    private[tss] var cursor: Int = 0
  }

}
