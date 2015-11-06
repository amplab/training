/*

/**
 * Created by cusgadmin on 6/9/15.
 */

import breeze.linalg._
import breeze.plot.{Figure, image}
import breeze.stats.distributions.Gaussian
import main.scala.ioTools.ReadCsv
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import main.scala.overlapping._
import main.scala.overlapping.timeSeries.TSInstant

import scala.math.Ordering

object RunWithUberDemandData {

  def main(args: Array[String]): Unit = {

    val filePath = "/users/cusgadmin/traffic_data/uber-ny/uber_spatial_bins_20x20_merged.csv"

    val data = ReadCsv(filePath, 0, "yyyy-MM-dd HH:mm:ss", true)

    val d             = data.head._2.length
    val nSamples      = data.length
    val paddingMillis = 6000000L // 100 minutes
    val deltaTMillis  = 60000L // 1 minute
    val nPartitions   = 8

    val conf = new SparkConf().setAppName("Counter").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rawTS = sc.parallelize(data)

    implicit val DateTimeOrdering = new Ordering[(DateTime, Array[Double])] {
      override def compare(a: (DateTime, Array[Double]), b: (DateTime, Array[Double])) =
        a._1.compareTo(b._1)
    }

    val signedDistance = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

    val (overlappingRDD: RDD[(Int, SingleAxisBlock[TSInstant, DenseVector[Double]])], intervals: Array[(TSInstant, TSInstant)]) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), signedDistance, nPartitions, rawTS)

    /*
     Estimate process' mean
     */
    val meanEstimator = new MeanEstimator[TSInstant](d)
    val secondMomentEstimator = new SecondMomentEstimator[TSInstant](d)

    val mean = meanEstimator.estimate(overlappingRDD)

    /*
    Monovariate analysis
     */
    val p = 1
    val freqAREstimator = new ARModel[TSInstant](deltaTMillis, 1, d, sc.broadcast(mean))
    val vectorsAR = freqAREstimator.estimate(overlappingRDD)

    val predictorAR = new ARPredictor[TSInstant](
      deltaTMillis,
      1,
      d,
      sc.broadcast(mean),
      sc.broadcast(vectorsAR.map(x => x.covariation)))

    val predictionsAR = predictorAR.predictAll(overlappingRDD)
    val residualsAR = predictorAR.residualAll(overlappingRDD)
    val residualMeanAR = meanEstimator.estimate(residualsAR)
    val residualSecondMomentAR = secondMomentEstimator.estimate(residualsAR)

    println(residualMeanAR)
    println(trace(residualSecondMomentAR))

    val f1 = Figure()
    f1.subplot(0) += image(residualSecondMomentAR)
    f1.saveas("residuals_AR.png")

    /*
    Multivariate analysis
     */
    val freqVAREstimator = new VARModel[TSInstant](deltaTMillis, 1, d, sc.broadcast(mean))
    val (freqVARmatrices, covMatrix) = freqVAREstimator.estimate(overlappingRDD)

    val predictorVAR = new VARPredictor[TSInstant](
      deltaTMillis,
      1,
      d,
      sc.broadcast(mean),
      sc.broadcast(freqVARmatrices))

    val predictionsVAR = predictorVAR.predictAll(overlappingRDD)
    val residualsVAR = predictorVAR.residualAll(overlappingRDD)
    val residualMeanVAR = meanEstimator.estimate(residualsVAR)
    val residualSecondMomentVAR = secondMomentEstimator.estimate(residualsVAR)

    println(residualMeanVAR)
    println(trace(residualSecondMomentVAR))

    val f2 = Figure()
    f2.subplot(0) += image(residualSecondMomentVAR)
    f2.saveas("residuals_VAR.png")

    val f3 = Figure()
    f3.subplot(0) += image(freqVARmatrices(0))
    f3.saveas("coeffs_VAR.png")

    /*
    Multivariate Bayesian analysis
     */
    val VARLoss = new DiagonalNoiseARLoss(diag(residualSecondMomentVAR), nSamples, sc.broadcast(mean))
    val VARGrad = new DiagonalNoiseARGrad(diag(residualSecondMomentVAR), nSamples, sc.broadcast(mean))

    val svd.SVD(_, s, _) = svd(covMatrix)

    def stepSize(x: Int): Double ={
      1.0 / (max(s) * max(diag(residualSecondMomentVAR)) + min(s) * min(diag(residualSecondMomentVAR)))
    }

    val VARBayesEstimator = new VARGradientDescent[TSInstant](
      p,
      deltaTMillis,
      new AutoregressiveLoss(
      p,
      deltaTMillis,
      Array.fill(p){DenseMatrix.zeros[Double](d, d)},
      {case (param, data) => VARLoss(param, data)}),
      new AutoregressiveGradient(
      p,
      deltaTMillis,
      Array.fill(p){DenseMatrix.zeros[Double](d, d)},
      {case (param, data) => VARGrad(param, data)}),
      stepSize,
      1e-5,
      1000,
      freqVARmatrices
    )

    val bayesianVAR = VARBayesEstimator.estimate(overlappingRDD)

    val f4 = Figure()
    f4.subplot(0) += image(bayesianVAR(0))
    f4.saveas("coeffs_bayesian_VAR.png")

    val sparseVARBayesEstimator = new VARL1GradientDescent[TSInstant](
      p,
      deltaTMillis,
      new AutoregressiveLoss(
      p,
      deltaTMillis,
      Array.fill(p){DenseMatrix.zeros[Double](d, d)},
      {case (param, data) => VARLoss(param, data)}),
      new AutoregressiveGradient(
      p,
      deltaTMillis,
      Array.fill(p){DenseMatrix.zeros[Double](d, d)},
      {case (param, data) => VARGrad(param, data)}),
      stepSize,
      1e-5,
      1e-2,
      1000,
      freqVARmatrices
    )

    val sparseBayesianVAR = sparseVARBayesEstimator.estimate(overlappingRDD)

    val f5 = Figure()
    f5.subplot(0) += image(sparseBayesianVAR(0))
    f5.saveas("coeffs_sparse_bayesian_VAR.png")


  }
}

*/