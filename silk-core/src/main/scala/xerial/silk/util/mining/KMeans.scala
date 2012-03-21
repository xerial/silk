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
import collection.{GenTraversable, GenTraversableOnce, GenSeq}
import collection.generic.FilterMonadic
import collection.immutable.IndexedSeq
import java.util.concurrent.ConcurrentSkipListSet

//--------------------------------------
//
// KMeans.scala
// Since: 2012/03/15 22:15
//
//--------------------------------------

class ClusteringInput[T](val point: Array[T], val metric: PointDistance[T]) {

  val N = point.length

  def uniqueVectors = {
    point.par.map(x => metric.getVector(x)).distinct
  }

  def vectors = point.par.map(x => metric.getVector(x))

}

class Cluster[T](val input: ClusteringInput[T], val centroid: Array[Array[Double]], val clusterAssignment: Array[Int]) {

  implicit def toVector(p: T): Array[Double] = metric.getVector(p)

  val N = input.N
  val point = input.point
  val metric = input.metric

  /**
   * The number of clusters
   */
  val K = centroid.length

  private def pointIDsInCluster(clusterID: Int): FilterMonadic[Int, IndexedSeq[Int]] = {
    (0 until input.N).withFilter(i => clusterAssignment(i) == clusterID)
  }

  def pointVectors = input.point.map(metric.getVector(_))

  def pointsInCluster(clusterID: Int): Seq[T] = {
    pointIDsInCluster(clusterID).map(x => point(x))
  }

  def pointVectorsInCluster(clusterID: Int): Seq[Array[Double]] = {
    pointIDsInCluster(clusterID).map(x => metric.getVector(point(x)))
  }

  lazy val sumOfSquareError: Double = {

    def distToCentroid(pid: Int) = {
      val cid = clusterAssignment(pid)
      val centroid = centroid(cid)
      val point = input.point(pid)
      Math.pow(metric.distance(centroid, metric.getVector(point)), 2)
    }

    val distSum = (0 until input.N).par.map(distToCentroid(_)).sum
    distSum
  }

  lazy val averageOfDistance: Double = {
    sumOfSquareError / N
  }

  def findClosestCentroidID(p: T): Int = {
    val dist = (0 until K).map {
      cid => (cid, metric.distance(p, centroid(cid)))
    }
    val min = dist.minBy {
      case (cid, d) => d
    }
    min._1
  }

  def reassignToClosestCentroid: Cluster[T] = {
    val newClusterAssignment: Array[Int] = Array.fill(N)(-1)
    (0 until N).par.foreach {
      i =>
        newClusterAssignment(i) = findClosestCentroidID(point(i))
    }
    new Cluster(input, centroid, newClusterAssignment)
  }

  def updateCentroids(newCentroid: Array[Array[Double]]) =
    new Cluster(input, newCentroid, clusterAssignment)

}

/**
 * Distance definition of data points for K-Means clustering. Use [[xerial.silk.util.mining.EuclidPointDistance]] to implement this trait.
 *
 * @author leo
 *
 */
trait PointDistance[T] {

  type PointVector = Array[Double]

  /**
   * Get point vector of the element
   * @param e
   * @return
   */
  def getVector(e: T): PointVector

  /**
   * The size of dimension;
   *
   * @return
   */
  def dimSize: Int

  /**
   * Return the distance between the given two points
   *
   * @param a
   * @param b
   * @return |a-b|
   */
  def distance(a: PointVector, b: PointVector): Double

  /**
   * Return the center of mass of the inputs
   *
   * @param points
   *            list of input points
   * @return |a+b+c+... | / N
   */
  def centerOfMass(points: GenTraversableOnce[PointVector]): PointVector


  /**
   * Compute the lower bound of the points
   *
   * @param points
   * @return
   */
  def lowerBound(points: GenTraversableOnce[PointVector]): PointVector

  /**
   * Compute the upper bound of the points
   *
   * @param points
   * @return
   */
  def upperBound(points: GenTraversableOnce[PointVector]): PointVector

  /**
   * Move the points to the specified direction to the amount of the given distance
   *
   * @param point
   * @return
   */
  def move(point: PointVector, direction: PointVector): PointVector
}

/**
 * Standard Euclid point distance implementation
 * @tparam T
 */
trait EuclidPointDistance[T] extends PointDistance[T] {

  def distance(a: PointVector, b: PointVector): Double = {
    val sum = (0 until dimSize).par.map(col => Math.pow(a(col) - b(col), 2)).sum
    Math.sqrt(sum)
  }
  def centerOfMass(points: GenTraversableOnce[PointVector]): PointVector = {
    var n = 0
    val v = points.reduce((a, b) => {
      n += 1;
      a.zip(b).map(x => x._1 + x._2)
    })
    v.map(x => x / n)
  }
  def lowerBound(points: GenTraversableOnce[PointVector]): PointVector = {
    points.reduce((a, b) => a.zip(b).map(x => Math.min(x._1, x._2)))
  }
  def upperBound(points: GenTraversableOnce[PointVector]): PointVector = {
    points.reduce((a, b) => a.zip(b).map(x => Math.max(x._1, x._2)))
  }
  def move(point: PointVector, direction: PointVector): PointVector = {
    point.zip(direction).map(x => x._1 + x._2)
  }
}

object KMeans {

  class Config {
    var maxIteration: Int = 300
  }

  def apply[T](K: Int, point: Array[T], metric: PointDistance[T]): Cluster[T] = {
    val kmeans = new KMeans(new ClusteringInput[T](point, metric))
    kmeans.execute(K)
  }

}

/**
 * K-means clustering
 *
 * @author leo
 *
 */
class KMeans[T](input: ClusteringInput[T], config: KMeans.Config = new KMeans.Config) extends Logging {

  type PointVector = Array[Double]
  private val random: Random = new Random(0)

  private def hasDuplicate(points: GenSeq[PointVector]): Boolean = {
    val uniquePoints = points.distinct
    points.length != uniquePoints.length
  }

  /**
   * Randomly choose K-centroids from the input data set
   *
   * @param K
   * @return
   */
  protected def initCentroids(K: Int): Array[PointVector] = {
    if (K > input.N)
      throw new IllegalArgumentException("K(=%d) must be smaller than N(%d)".format(K, input.N))

    val uniquePoints = input.uniqueVectors
    val UN = uniquePoints.length

    if(UN <= K)
      throw new IllegalArgumentException("K(=%d) must be larger than the number of unique points".format(K))

    def pickCentroid(centroids: List[PointVector], remaining: Int): List[PointVector] = {
      if (remaining == 0)
        centroids
      else {
        val r = random.nextInt(UN)
        val v = uniquePoints(r)
        pickCentroid(v :: centroids, remaining - 1)
      }
    }

    pickCentroid(List(), K).toArray
  }

  /**
   * @param K
   *            number of clusters
   * @throws Exception
   */
  def execute(K: Int): Cluster[T] = {
    execute(K, initCentroids(K))
  }

  /**
   * @param K
   *            number of clusters
   * @param centroid
   *            initial centroids
   * @throws Exception
   */
  def execute(K: Int, centroid: Array[PointVector]): Cluster[T] = {

    /**
     * Returns the list of new centroids by taking the center of mass of the points in the clusters
     *
     * @param cluster
     * @return
     */
    def MStep(cluster: Cluster[T]): Array[PointVector] = {
      val newCentroids = (0 until cluster.K).par.map {
        cid =>
          cluster.metric.centerOfMass(cluster.pointVectorsInCluster(cid))
      }
      newCentroids.toArray
    }

    /**
     * K-means clustering iteration
     * @param i
     * @param cluster
     * @return
     */
    def iteration(i: Int, cluster: Cluster[T]): Cluster[T] = {
      debug("iteration: %d", i + 1)
      if (i >= config.maxIteration)
        cluster
      else {
        // E-step: Assign each point to the closest centroid
        val newCluster = cluster.reassignToClosestCentroid
        if (newCluster.averageOfDistance >= cluster.averageOfDistance) {
          cluster
        }
        else {
          // M-step: update the centroids
          val newCentroids = MStep(newCluster)
          if (hasDuplicate(newCentroids))
            cluster
          else
            iteration(i + 1, newCluster.updateCentroids(newCentroids))
        }
      }
    }

    iteration(0, new Cluster[T](input, centroid.toArray, Array.fill(input.N)(0)))
  }

}
