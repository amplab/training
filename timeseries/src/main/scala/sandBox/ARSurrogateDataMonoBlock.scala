/*

/**
 * Created by cusgadmin on 6/9/15.
 */

import breeze.linalg._
import breeze.plot._
import breeze.stats.distributions.Gaussian
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import main.scala.overlapping.containers.block.SingleAxisBlock
import main.scala.overlapping.io.SingleAxisBlockRDD
import main.scala.overlapping.timeSeries.firstOrder.{SecondMomentEstimator, MeanEstimator}
import main.scala.overlapping.timeSeries.secondOrder.multivariate.VARPredictor
import main.scala.overlapping.timeSeries.secondOrder.multivariate.bayesianEstimators.gradients.DiagonalNoiseARGrad
import main.scala.overlapping.timeSeries.secondOrder.multivariate.bayesianEstimators.lossFunctions.DiagonalNoiseARLoss
import main.scala.overlapping.timeSeries.secondOrder.multivariate.bayesianEstimators.{AutoregressiveGradient, AutoregressiveLoss, VARGradientDescent}
import main.scala.overlapping.timeSeries.secondOrder.multivariate.frequentistEstimators._
import main.scala.overlapping.timeSeries.surrogateData.{IndividualRecords, TSInstant}

import scala.math.Ordering

object ARSurrogateDataMonoBlock {

  def main(args: Array[String]): Unit ={

    /*
     #################################################

      Generate surrogate data

     ################################################
     */

    val d             = 3
    val nSamples      = 1000000L
    val paddingMillis = 1000L
    val deltaTMillis  = 1L
    val nPartitions   = 8

    val conf  = new SparkConf().setAppName("Counter").setMaster("local[*]")
    val sc    = new SparkContext(conf)

    val ARcoeffs = Array(DenseMatrix((0.25, 0.0, -0.01), (0.15, -0.30, 0.04), (0.0, -0.15, 0.35)),
      DenseMatrix((0.06, 0.03, 0.0), (0.0, 0.07, -0.09), (0.0, 0.0, 0.07)))

    val rawTS = IndividualRecords.generateVAR(
      ARcoeffs,
      d, nSamples.toInt, deltaTMillis,
      Gaussian(0.0, 1.0),
      DenseVector(1.0, 1.0, 1.0),
      sc)

    implicit val DateTimeOrdering = new Ordering[(DateTime, Array[Double])] {
      override def compare(a: (DateTime, Array[Double]), b: (DateTime, Array[Double])) =
        a._1.compareTo(b._1)
    }

    val signedDistance = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

    val (overlappingRDD: RDD[(Int, SingleAxisBlock[TSInstant, DenseVector[Double]])], intervals: Array[(TSInstant, TSInstant)]) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), signedDistance, nPartitions, rawTS)

    /*
     #################################################

      Estimation of process mean

     ################################################
     */

    val p = ARcoeffs.length

    val meanEstimator = new MeanEstimator[TSInstant](d)
    val mean = meanEstimator.estimate(overlappingRDD)

    println("Results of AR multivariate frequentist estimator")

    println("Mean = ")
    println(mean)

    /*
     #################################################

      Frequentist estimation of AR coeffs
      and noise variance-covariance matrix

     ################################################
     */

    val VAREstimator = new VARModel[TSInstant](1.0, p, d, sc.broadcast(mean))
    val (coeffMatricesAR, noiseVarianceAR) = VAREstimator
      .estimate(overlappingRDD)

    //val f1 = Figure()
    //f1.subplot(0) += image(coeffMatricesAR(0))
    //f1.saveas("AR_1_surrogate.png")

    println("AR estimated model:")
    coeffMatricesAR.foreach(x=> {println(x); println()})
    println(noiseVarianceAR)
    println()

    /*
     #################################################

      Estimation of ML estimator's Hessian

     ################################################
     */

    val sigmaEpsDiag: DenseVector[Double] = diag(noiseVarianceAR)

    val crossCovEstimator = new CrossCovariance[TSInstant](1.0, p, d, sc.broadcast(DenseVector.zeros(d)))
    val (crossCovMatrices, covMatrix) = crossCovEstimator
      .estimate(overlappingRDD)

    crossCovMatrices.foreach(x=> {println(x); println()})

    val svd.SVD(_, s, _) = svd(covMatrix)

    def stepSize(x: Int): Double ={
      1.0 / (max(s) * max(sigmaEpsDiag) + min(s) * min(sigmaEpsDiag))
    }

    /*
     #################################################

      Setup gradient descent

     ################################################
     */

    val VARLoss = new DiagonalNoiseARLoss(sigmaEpsDiag, nSamples, sc.broadcast(mean))
    val VARGrad = new DiagonalNoiseARGrad(sigmaEpsDiag, nSamples, sc.broadcast(mean))

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
      Array.fill(p){DenseMatrix.zeros(d, d)}
    )

    val ARMatrices = VARBayesEstimator.estimate(overlappingRDD)

    /*
     #################################################

      Setup predictions

     ################################################
     */

    val predictor = new VARPredictor[TSInstant](
      deltaTMillis,
      1,
      d,
      sc.broadcast(mean),
      sc.broadcast(ARMatrices))

    println("Starting predictions")
    val startTime1 = System.currentTimeMillis()
    val predictions = predictor.predictAll(overlappingRDD)
    println("Done after " + (System.currentTimeMillis() - startTime1) + " milliseconds")

    println("Computing residuals")
    val startTime2 = System.currentTimeMillis()
    val residuals = predictor.residualAll(overlappingRDD)
    println("Done after " + (System.currentTimeMillis() - startTime2) + " milliseconds")

    println("Computing mean residual")
    val startTime3 = System.currentTimeMillis()
    val residualMean = meanEstimator.estimate(residuals)
    println("Done after " + (System.currentTimeMillis() - startTime3) + " milliseconds")
    println(residualMean)

    val secondMomentEstimator = new SecondMomentEstimator[TSInstant](d)

    println("Computing mean squared residual")
    val startTime4 = System.currentTimeMillis()
    val residualSecondMoment = secondMomentEstimator.estimate(residuals)
    println("Done after " + (System.currentTimeMillis() - startTime4) + " milliseconds")
    println(residualSecondMoment)

    //println(residualMean)
    //println(sigmaEps)

    //val f2 = Figure()
    //f2.subplot(0) += image(sigmaEps)
    //f2.saveas("sigma_eps_surrogate.png")

    //val f3 = Figure()
    //f3.subplot(0) += image(noiseVarianceAR)
    //f3.saveas("noise_variance_AR_surrogate.png")

    println()

  }
}

*/