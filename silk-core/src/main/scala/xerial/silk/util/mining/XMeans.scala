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

import collection.GenSeq
import xerial.silk.util.Logging
import java.util.{ArrayList, Random}
import annotation.tailrec

//--------------------------------------
//
// XMeans.scala
// Since: 2012/03/19 15:20
//
//--------------------------------------

class XMeansCluster[T]
(
  val BIC: Double,
  input: ClusteringInput[T],
  centroid: Array[Array[Double]],
  clusterAssignment: Array[Int])
  extends Cluster[T](input, centroid, clusterAssignment) {
  def this(BIC: Double, cluster: Cluster[T]) = this(BIC, cluster.input, cluster.centroid, cluster.clusterAssignment)

}

object XMeans {

  /**
   * Compute Bayesian Information Criteria (BIC) of the clusters
   *
   * @return BIC value
   */
  protected def computeBIC[T](cluster: Cluster[T]): Double = {
    val R: Double = cluster.N
    val K: Double = cluster.K
    if (R <= cluster.K) {
      return Double.MinValue
    }
    val sigmaSquare: Double = cluster.sumOfSquareError / (R - K)
    val M: Int = cluster.metric.dimSize

    def loop(k: Int, bic: Double): Double = {
      if (k >= K)
        bic
      else {
        val R_n = cluster.clusterAssignment.count(cid => cid == k)
        val p1: Double = -((R_n / 2.0) * Math.log(2.0 * Math.Pi))
        val p2: Double = -((R_n * M) / 2.0) * Math.log(sigmaSquare)
        val p3: Double = -(R_n - K) / 2.0
        val p4: Double = R_n * Math.log(R_n)
        val p5: Double = -R_n * Math.log(R)
        val likelihoodOfTheCluster: Double = p1 + p2 + p3 + p4 + p5

        // This part accumulates penalties on the number of free parameters for each bic computation.
        // This is not exactly the same with the original X-means paper by D. Pelleg, but
        // without this accumulation, the penalty of adding new cluster becomes inadequately low.
        val numberOfFreeParameters: Int = ((K - 1) + M * K + 1).asInstanceOf[Int]
        val newBIC = bic + likelihoodOfTheCluster - (numberOfFreeParameters / 2.0) * Math.log(R_n)
        loop(k + 1, newBIC)
      }
    }
    // Compute the sum of the cluster likelihood
    val bic = loop(0, 0.0)
    if (bic.isNaN)
      Double.MinValue
    else
      bic
  }

}

/**
 *
 * Reference:
 * X-means: Extending K-means with efficient estimation of the number of clusters. D. Pelleg et al.
 *
 * @author leo
 */
class XMeans[T](input: ClusteringInput[T]) extends Logging {

  type PointVector = Array[Double]
  private val rand: Random = new Random(0)

  import XMeans._

  /**
   * @param maxK
   *            maximum number of the cluster
   * @return
   * @throws Exception
   */
  def execute(maxK: Int): XMeansCluster[T] = {
    val centroid = input.metric.centerOfMass(input.vectors)
    val cluster: Cluster[T] = new Cluster[T](input, Array(centroid), Array.fill(input.N)(0))
    val BIC = computeBIC(cluster)
    iteration(new XMeansCluster(BIC, input, cluster.centroid, cluster.clusterAssignment), maxK)
  }

  protected def iteration(initCluster: XMeansCluster[T], maxK: Int): XMeansCluster[T] = {

    def EStep(K: Int, cluster: XMeansCluster[T]): XMeansCluster[T] = {
      val kmeans: KMeans[T] = new KMeans[T](cluster.input)
      val kmeansCluster = kmeans.execute(K)
      val BIC = computeBIC(kmeansCluster)
      return new XMeansCluster(BIC, kmeansCluster)
    }

    def splitCentroids(cluster: XMeansCluster[T]): Array[PointVector] = {
      @tailrec
      def loop(k: Int, centroids: List[PointVector]): List[PointVector] = {
        if (k >= cluster.K)
          centroids
        else {
          val pointsInTheCluster = cluster.pointsInCluster(k).toArray
          val singleCluster = new Cluster[T](new ClusteringInput(pointsInTheCluster, input.metric), Array(cluster.centroid(k)), Array.fill(pointsInTheCluster.length)(0))
          val currentBIC = computeBIC(singleCluster)
          // Split the cluster into two, then perform 2-means
          val newCluster: Cluster[T] = splitCluster(singleCluster)
          val newBIC = computeBIC(newCluster)
          if (newCluster.K == 1 || newBIC < currentBIC) {
            // Using current centroid as is
            loop(k + 1, singleCluster.centroid(0) :: centroids)
          }
          else {
            // Splitting the centroid into two has better BIC
            loop(k + 1, newCluster.centroid.toList ::: centroids)
          }
        }
      }
      val newCentroids = loop(0, List())
      newCentroids.distinct.toArray // remove duplicate points
    }

    def loop(current: XMeansCluster[T]): XMeansCluster[T] = {
      debug("# of clusters = %d", current.K)
      if (current.K >= maxK)
        current
      else {
        val nextCentroids = splitCentroids(current)
        val nextCluster = EStep(nextCentroids.length, current)
        if (current.BIC >= nextCluster.BIC)
          current
        else
          loop(nextCluster)
      }
    }

    loop(initCluster)
  }

  /**
   * Split the centroid of the input cluster (K=1) into two, then perform 2-means clustering
   * @param cluster
   * @return
   */
  protected def splitCluster(cluster: Cluster[T]): Cluster[T] = {
    assert(cluster.K == 1)
    val lowerBound: PointVector = input.metric.lowerBound(cluster.pointVectors)
    val upperBound: PointVector = input.metric.upperBound(cluster.pointVectors)
    val diameter: Double = {
      val d = input.metric.distance(lowerBound, upperBound)
      if (d.isInfinite)
        Double.MaxValue / 2.0
      else
        d
    }

    def splitCentroid = {
      val D = input.metric.dimSize

      val direction: Array[Double] = Array.fill(D)(rand.nextDouble)
      val sumSquare = Math.sqrt(direction.sum)
      val factor: Double = diameter / sumSquare
      val d = direction.map(_ * factor)
      val r = direction.map(_ * -factor)

      val c = cluster.centroid(0)
      // Move two centroids to the opposite directions from the centroid c
      Array(input.metric.move(c, d), input.metric.move(c, r))
    }

    if (diameter == 0.0) {
      cluster
    }
    else {
      val newCentroid = splitCentroid
      val kmeans: KMeans[T] = new KMeans[T](cluster.input)
      kmeans.execute(newCentroid.length, newCentroid)
    }
  }
}