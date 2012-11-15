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
import xerial.core.log.Logger

//--------------------------------------
//
// XMeans.scala
// Since: 2012/03/19 15:20
//
//--------------------------------------

class XMeansCluster[T:ClassManifest]
(
  val BIC: Double,
  input: ClusteringInput[T],
  centroid: Array[DVector],
  clusterAssignment: Array[Int])
  extends Cluster[T](input, centroid, clusterAssignment) {
  def this(BIC: Double, cluster: Cluster[T]) = this(BIC, cluster.input, cluster.centroid, cluster.clusterAssignment)

}

object XMeans {

  /**
   * Compute Bayesian Information Criteria (BIC) of the clusters.
   * The original X-means paper assumes that each cluster follows the identical distribution.
   *
   * In this implementation, we assume multiple Gaussian distributions.
   *
   * @return BIC value
   */
  protected def computeBIC[T](cluster: Cluster[T]): Double = {
    val R: Double = cluster.N
    val K: Double = cluster.K
    if (R <= cluster.K) {
      return Double.MinValue
    }
    //val sigmaSquare: Double = cluster.sumOfSquareError / (R - K)
    val M: Int = cluster.metric.dimSize

    def likelihood(k: Int): Double = {
      val point = cluster.pointVectorsInCluster(k).toArray
      val R_n = point.length
      // -1 is for the centroid
      val varianceSquare = cluster.metric.squaredSumOfDistance(point, cluster.centroid(k)) / (R_n - 1.0)
      val p1: Double = -((R_n / 2.0) * math.log(2.0 * math.Pi))
      val p2: Double = -((R_n * M) / 2.0) * math.log(varianceSquare)
      val p3: Double = -(R_n - K) / 2.0
      val p4: Double = R_n * math.log(R_n)
      val p5: Double = -R_n * math.log(R)
      val likelihoodOfTheCluster: Double = p1 + p2 + p3 + p4 + p5
      likelihoodOfTheCluster
    }

    // Accumulate the likelihood of the clusters
    val likelihoodSum = (0 until cluster.K).par.map {
      cid => likelihood(cid)
    }.sum

    // K: the number of clusters(centroids), M: dim size
    // -- The number of free parameters
    // K-1: the number of cluster assignment probabilities (p_1, ...p_K: where p_K = 1 - (p_1 + p_2 + ... + p_{K-1}))
    // K: the number of variance parameters
    // K * M: the number of parameters for centroid vectors of M dimension,
    val numberOfFreeParameters = K - 1 + K + K * M

    val bic = likelihoodSum - (numberOfFreeParameters / 2.0) * math.log(R)

    if (bic.isNaN) Double.MinValue else bic
  }

}

/**
 *
 * Reference:
 * X-means: Extending K-means with efficient estimation of the number of clusters. D. Pelleg et al.
 *
 * @author leo
 */
class XMeans[T:ClassManifest](input: ClusteringInput[T]) extends Logger {

  private val rand: Random = new Random(0)

  private val metric = input.metric

  import XMeans._

  /**
   * @param maxK
   *            maximum number of the cluster
   * @return
   * @throws Exception
   */
  def execute(maxK: Int): XMeansCluster[T] = {
    val centroid = input.metric.centerOfMass(input.vectors)
    val cluster: Cluster[T] = new Cluster[T](input, Array(centroid), Array.fill[Int](input.N)(0))
    val BIC = computeBIC(cluster)
    iteration(new XMeansCluster(BIC, input, cluster.centroid, cluster.clusterAssignment), maxK)
  }

  protected def iteration(initCluster: XMeansCluster[T], maxK: Int): XMeansCluster[T] = {

    def EStep(centroid: Array[DVector]): XMeansCluster[T] = {
      val cluster = initCluster.updateCentroids(centroid)
      val kmeans: KMeans[T] = new KMeans[T](cluster.input)
      val kmeansCluster = kmeans.execute(cluster.K)
      val BIC = computeBIC(kmeansCluster)
      return new XMeansCluster(BIC, kmeansCluster)
    }

    def splitCentroids(cluster: XMeansCluster[T]): Array[DVector] = {
      // For each cluster, chose next one or two centroids
      val newCentroid = (0 until initCluster.K).par.flatMap {
        cid =>
          val c = initCluster.extractCluster(cid)
          val currentBIC = computeBIC(c)
          // Split the cluster into two
          val newBIC = computeBIC(c)
          val newCluster = splitCluster(c)
          if (newCluster.K == 1 || newBIC < currentBIC)
            c.centroid // Using current centroid as is
          else
            newCluster.centroid // Splitting the centroid into two has better (larger) BIC
      }
      newCentroid.distinct.toArray
    }

    def loop(current: XMeansCluster[T]): XMeansCluster[T] = {
      debug("# of clusters = %d", current.K)
      if (current.K >= maxK)
        current
      else {
        // Test split or keep centroids
        val nextCentroids = splitCentroids(current)
        // Run global k-means using new centroids
        val nextCluster = EStep(nextCentroids)
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
    val lowerBound: DVector = metric.lowerBound(cluster.pointVectors)
    val upperBound: DVector = metric.upperBound(cluster.pointVectors)
    val diameter: Double = {
      val d = metric.distance(lowerBound, upperBound)
      if (d.isInfinite)
        Double.MaxValue / 2.0
      else
        d
    }

    def splitCentroid = {
      val direction: DVector = DVector.fill(input.metric.dimSize)(rand.nextDouble)
      val length: Double = diameter / math.sqrt(direction.sum)
      val c = cluster.centroid(0)
      // Move two centroids to the opposite directions from the centroid c
      Array(c + (direction * length), c + (direction * -length))
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
