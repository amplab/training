package main.scala.sandBox

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


object BayesianMAp {

  implicit def signedDistMillis = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

  implicit def signedDistLong = (t1: Long, t2: Long) => (t2 - t1).toDouble

  def main(args: Array[String]): Unit = {

    val d = 3
    val N = 100000L
    val paddingMillis = 100L
    val deltaTMillis = 1L
    val nPartitions = 8

    implicit val config = TSConfig(deltaTMillis, d, N, paddingMillis.toDouble)

    val conf = new SparkConf().setAppName("Counter").setMaster("local[*]")
    implicit val sc = new SparkContext(conf)

    val MACoeffs = Array(
      DenseMatrix((0.30, 0.0, 0.0), (0.0, -0.20, 0.0), (0.0, 0.0, -0.45)),
      DenseMatrix((0.12, 0.0, 0.0), (0.0, 0.08, 0.0), (0.0, 0.0, 0.45)),
      DenseMatrix((-0.08, 0.0, 0.0), (0.0, 0.05, 0.0), (0.0, 0.0, 0.0)),
      DenseMatrix((0.0, 0.0, 0.0), (0.0, 0.0, 0.0), (0.0, 0.0, 0.0)),
      DenseMatrix((0.0, 0.0, 0.0), (0.0, 0.0, 0.0), (0.0, 0.0, 0.0)),
      DenseMatrix((0.0, 0.0, 0.0), (0.0, 0.0, 0.0), (0.0, 0.0, 0.0)),
      DenseMatrix((0.0, 0.0, 0.0), (0.0, 0.0, 0.0), (0.0, 0.0, 0.0)),
      DenseMatrix((0.0, 0.0, 0.0), (0.0, 0.0, 0.0), (0.0, 0.0, 0.0)),
      DenseMatrix((0.0, 0.0, 0.0), (0.0, 0.0, 0.0), (0.0, 0.0, 0.0)),
      DenseMatrix((0.0, 0.0, 0.0), (0.0, 0.0, 0.0), (0.0, 0.0, 0.0)),
      DenseMatrix((0.0, 0.0, 0.0), (0.0, 0.0, 0.0), (0.0, 0.0, 0.0)),
      DenseMatrix((0.0, 0.0, 0.0), (0.0, 0.0, 0.0), (0.0, 0.0, 0.0)),
      DenseMatrix((0.0, 0.0, 0.0), (0.0, 0.0, 0.0), (0.0, 0.0, 0.0)),
      DenseMatrix((0.0, 0.0, 0.0), (0.0, 0.0, 0.0), (0.0, 0.0, 0.0)),
      DenseMatrix((0.0, 0.0, 0.0), (0.0, 0.0, 0.0), (0.0, 0.0, 0.0))
    )

    val maxGain = Stability(MACoeffs)
    if(maxGain > 1.0){
      println("Model is unstable (non invertible) with maximum gain = " + maxGain)
    }else{
      println("Model is stable (invertible) with maximum gain = " + maxGain)
    }

    val noiseMagnitudes = DenseVector.ones[Double](d)

    val rawTS = Surrogate.generateVMA(
      MACoeffs,
      d,
      N.toInt,
      deltaTMillis,
      Gaussian(0.0, 1.0),
      noiseMagnitudes,
      sc)

    val (overlappingRDD: RDD[(Int, SingleAxisBlock[TSInstant, DenseVector[Double]])], _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), nPartitions, rawTS)

    overlappingRDD.persist()

    /*
    ##################################

    Multivariate analysis

    ##################################
     */

    for(p <- 1 to MACoeffs.length) {

      var error = 0.0
      var tot_time = 0.0
      for(i <- 1 to 100) {
        val startTimeFreq = System.currentTimeMillis()
        val (freqVMAMatrices, _) = VMAModel(overlappingRDD, p)
        val elapsedTimeFreq = System.currentTimeMillis() - startTimeFreq

        tot_time += elapsedTimeFreq
        error = sum(freqVMAMatrices.indices.map(i => sum(abs(freqVMAMatrices(i) - MACoeffs(i)))))
      }

      println("Frequentist MA L1 estimation error (p = " + p + "), took " + tot_time / 100 + " millis)")
      println(error)
      println()

      /*
      val startTimeBayesian = System.currentTimeMillis()
      val denseVMAMatrices = VMAGradientDescent(overlappingRDD, p)
      val elapsedTimeBayesian = System.currentTimeMillis() - startTimeBayesian

      println("Bayesian L1 estimation error (p = " + p + "), took " + elapsedTimeBayesian + " millis)")
      println(sum(denseVMAMatrices.indices.map(i => sum(abs(denseVMAMatrices(i) - MACoeffs(i))))))
      println()
      */

    }

    /*
    ################################

    Sparse Bayesian analysis

    ################################
     */

    /*
    val sparseVARMatrices = VARL1GradientDescent(overlappingRDD, p, 1e-2)

    println("Sparse Bayesian L1 estimation error")
    println(sum(abs(sparseVARMatrices(0) - ARCoeffs(0))))
    println()

    val residualsSparseVAR = VARPredictor(overlappingRDD, sparseVARMatrices, Some(mean))
    val residualSecondMomentSparseVAR = SecondMomentEstimator(residualsSparseVAR)

    println("Sparse VAR residuals")
    println(trace(residualSecondMomentSparseVAR))
    println()
    */

  }
}