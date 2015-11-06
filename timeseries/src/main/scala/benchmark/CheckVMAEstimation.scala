package main.scala.benchmark

/**
 * Created by cusgadmin on 6/9/15.
 */

import breeze.linalg._
import breeze.numerics.abs
import breeze.stats.distributions.Gaussian
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import main.scala.overlapping.containers._
import main.scala.overlapping.timeSeries._

object CheckVMAEstimation {

  implicit def signedDistMillis = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

  implicit def signedDistLong = (t1: Long, t2: Long) => (t2 - t1).toDouble

  def main(args: Array[String]): Unit = {

    val filePath = "/users/cusgadmin/traffic_data/uber-ny/uber_spatial_bins_20x20_merged.csv"

    val d = 30
    val b = 25
    val N0 = 100000L
    val N1 = 10 * N0
    val paddingMillis = 100L
    val deltaTMillis = 1L
    val nPartitions = 8

    implicit var config = TSConfig(deltaTMillis, d, N0, paddingMillis.toDouble)

    val conf = new SparkConf().setAppName("Counter").setMaster("local[*]")
    implicit val sc = new SparkContext(conf)

    val A = (DenseMatrix.eye[Double](d) * 0.40) + (DenseMatrix.rand[Double](d, d) * 0.05)

    val svd.SVD(_, sA, _) = svd(A)
    A :*= 1.0 / (max(sA) * 1.1)

    val MAcoeffs = Array(A)
    val noiseMagnitudes = DenseVector.ones[Double](d) + (DenseVector.rand[Double](d) * 0.05)

    val rawTS = Surrogate.generateVMA(
      MAcoeffs,
      d,
      N0.toInt,
      deltaTMillis,
      Gaussian(0.0, 1.0),
      noiseMagnitudes,
      sc)

    val (overlappingRDD: RDD[(Int, SingleAxisBlock[TSInstant, DenseVector[Double]])], _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), nPartitions, rawTS)

    /*
    ##################################

    Multivariate analysis

    ##################################
     */
    val p = 1

    val mean = MeanEstimator(overlappingRDD)

    val (freqVMAMatrices, noiseCov) = VMAModel(overlappingRDD, p)
    println(noiseCov)

    println("Frequentist L1 estimation error with " + N0 + " points.")
    println(sum(abs(freqVMAMatrices(0) - MAcoeffs(0))))
    println(sum(abs(noiseCov - diag(noiseMagnitudes))))
    println()

    /*
    ######################################

    With more data

    ######################################
     */

    config = TSConfig(deltaTMillis, d, N1, paddingMillis.toDouble)

    val newRawTS = Surrogate.generateVAR(
      MAcoeffs,
      d,
      N1.toInt,
      deltaTMillis,
      Gaussian(0.0, 1.0),
      noiseMagnitudes,
      sc)

    val (newOverlappingRDD: RDD[(Int, SingleAxisBlock[TSInstant, DenseVector[Double]])], _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), nPartitions, newRawTS)

    /*
    ##################################

    Multivariate analysis

    ##################################
     */

    val newMean = MeanEstimator(newOverlappingRDD)

    val (newFreqVMAMatrices, newNoiseCov) = VMAModel.apply(newOverlappingRDD, p, Some(newMean))
    println(newNoiseCov)

    println("Frequentist L1 estimation error with " + N1 + " timestamps.")
    println(sum(abs(newFreqVMAMatrices(0) - MAcoeffs(0))))
    println(sum(abs(newNoiseCov - diag(noiseMagnitudes))))
    println()

  }
}