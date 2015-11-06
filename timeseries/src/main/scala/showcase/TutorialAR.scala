package main.scala.showcase

/**
 * Created by cusgadmin on 6/9/15.
 */

import breeze.linalg._
import breeze.stats.distributions.Gaussian
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import main.scala.overlapping._
import containers._
import timeSeries._

object TutorialAR {

  def main(args: Array[String]): Unit = {

    val d = 3
    val N = 10000L
    val paddingMillis = 100L
    val deltaTMillis = 1L
    val nPartitions = 8
    val actualP = 3

    implicit val config = TSConfig(deltaTMillis, d, N, paddingMillis.toDouble)

    val conf = new SparkConf().setAppName("Counter").setMaster("local[*]")
    implicit val sc = new SparkContext(conf)

    implicit def signedDistMillis = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

    val ARCoeffs: Array[DenseMatrix[Double]] = Array.fill(actualP){DenseMatrix.rand[Double](d, d) - (DenseMatrix.ones[Double](d, d) * 0.5)}
    Stability.makeStable(ARCoeffs)

    println(Stability(ARCoeffs))

    ARCoeffs.foreach(x => {println(x); println()})

    val noiseMagnitudes = DenseVector.ones[Double](d)

    val rawTS = Surrogate.generateVAR(
      ARCoeffs,
      d,
      N.toInt,
      deltaTMillis,
      Gaussian(0.0, 1.0),
      noiseMagnitudes,
      sc)

    val (timeSeriesRDD: RDD[(Int, SingleAxisBlock[TSInstant, DenseVector[Double]])], _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), nPartitions, rawTS)

    PlotTS.showModel(ARCoeffs, Some("Actual parameters"))
    PlotTS(timeSeriesRDD, Some("In Sample Data"))

    /*
    ################################

    Correlation analysis

    ###############################
     */
    val (correlations, _) = CrossCorrelation(timeSeriesRDD, 6)
    PlotTS.showModel(correlations, Some("Cross correlation"))
    //correlations.foreach(x => {println(x); println})

    val (partialCorrelations, _) = PartialCrossCorrelation(timeSeriesRDD,6)
    PlotTS.showModel(partialCorrelations, Some("Partial cross correlation"))
    partialCorrelations.foreach(x => {println(x); println})

    /*
    ################################

    Monovariate analysis

    ################################
     */
    val p = actualP
    val mean = MeanEstimator(timeSeriesRDD)
    val vectorsAR = ARModel(timeSeriesRDD, p, Some(mean)).map(_.covariation)
    val residualsAR = ARPredictor(timeSeriesRDD, vectorsAR, Some(mean))
    val residualSecondMomentAR = SecondMomentEstimator(residualsAR)

    PlotTS.showUnivModel(vectorsAR, Some("Monovariate parameter estimates"))
    PlotTS(residualsAR, Some("Monovariate AR residual error"))

    println("AR in sample error = " + trace(residualSecondMomentAR))

    /*
    ##################################

    Multivariate analysis

    ##################################
     */

    val (estVARMatrices, _) = VARModel(timeSeriesRDD, p)

    PlotTS.showModel(estVARMatrices, Some("Multivariate parameter estimates"))

    val residualVAR = VARPredictor(timeSeriesRDD, estVARMatrices, Some(mean))

    val residualSecondMomentVAR = SecondMomentEstimator(residualVAR)

    PlotTS.showCovariance(residualSecondMomentAR, Some("Monovariate residual covariance"))
    PlotTS.showCovariance(residualSecondMomentVAR, Some("Multivariate residual covariance"))

    println("Frequentist VAR in sample error = " + trace(residualSecondMomentVAR))
    println()


  }
}