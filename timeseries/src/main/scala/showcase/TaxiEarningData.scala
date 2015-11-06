package main.scala.showcase

/**
 * Created by cusgadmin on 6/9/15.
 */

import breeze.linalg._
import breeze.plot._

import main.scala.ioTools.ReadCsv

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import main.scala.overlapping._
import containers._
import timeSeries._

object TaxiEarningData {

  def main(args: Array[String]): Unit = {

    /*
    ##########################################

      In sample analysis

    ##########################################
     */


    val conf = new SparkConf().setAppName("Counter").setMaster("local[*]")
    implicit val sc = new SparkContext(conf)

    implicit def signedDistMillis = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

    val inSampleFilePath = "/users/cusgadmin/traffic_data/new_york_taxi_data/taxi_earnings_resampled/all.csv"

    val (inSampleDataHD, _, nSamples) = ReadCsv.TS(inSampleFilePath)

    val inSampleData = inSampleDataHD.mapValues(v => v(60 until 120))

    val d = 60
    val deltaTMillis = 5 * 60L * 1000L // 5 minutes
    val paddingMillis  = deltaTMillis * 100L
    val nPartitions   = 8
    implicit val config = TSConfig(deltaTMillis, d, nSamples, paddingMillis.toDouble)

    println(nSamples + " samples")
    println(d + " dimensions")
    println()

    val (rawTimeSeries: RDD[(Int, SingleAxisBlock[TSInstant, DenseVector[Double]])], _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), nPartitions, inSampleData)

    /*
    ############################################

      Get rid of seasonality

    ############################################
     */

    def hashFunction(x: TSInstant): Int = {
      (x.timestamp.getDayOfWeek - 1) * 24 * 60 / 5 + (x.timestamp.getMinuteOfDay - 1) / 5
    }
    val matrixMeanProfile = DenseMatrix.zeros[Double](7 * 24 * 12, d)

    val meanProfile = MeanProfileEstimator(rawTimeSeries, hashFunction)

    for(k <- meanProfile.keys){
      matrixMeanProfile(k, ::) := meanProfile(k).t
    }

    PlotTS.showProfile(matrixMeanProfile, Some("Weekly demand profile"), Some("Weekly_demand_profile.png"))

    val noSeason = MeanProfileEstimator.removeSeason(inSampleData, hashFunction, meanProfile)

    val (timeSeriesRDD: RDD[(Int, SingleAxisBlock[TSInstant, DenseVector[Double]])], _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), nPartitions, noSeason)

    /*
    ################################

    Correlation analysis

    ###############################
     */
    val (correlations, _) = CrossCorrelation(timeSeriesRDD, 6)
    PlotTS.showModel(correlations, Some("Cross correlation"), Some("Correlations_taxis.png"))
    //correlations.foreach(x => {println(x); println})

    val (partialCorrelations, _) = PartialCrossCorrelation(timeSeriesRDD,6)
    PlotTS.showModel(partialCorrelations, Some("Partial cross correlation"), Some("Partial_correlation_taxis.png"))
    partialCorrelations.foreach(x => {println(x); println})

    val chosenP = 3

    /*
    ################################

    Monovariate analysis

    ################################
     */

    val mean = MeanEstimator(timeSeriesRDD)
    val vectorsAR = ARModel(timeSeriesRDD, chosenP, Some(mean)).map(_.covariation)
    val residualsAR = ARPredictor(timeSeriesRDD, vectorsAR, Some(mean))
    val residualSecondMomentAR = SecondMomentEstimator(residualsAR)

    PlotTS.showUnivModel(vectorsAR, Some("Monovariate parameter estimates"), Some("Univariate_model_taxis.png"))

    println("AR in sample error = " + trace(residualSecondMomentAR))

    /*
    ##################################

    Multivariate analysis

    ##################################
     */

    val (estVARMatrices, _) = VARModel(timeSeriesRDD, chosenP)

    PlotTS.showModel(estVARMatrices, Some("Multivariate parameter estimates"), Some("VAR_model_taxis.png"))

    val residualVAR = VARPredictor(timeSeriesRDD, estVARMatrices, Some(mean))

    val residualSecondMomentVAR = SecondMomentEstimator(residualVAR)

    PlotTS.showCovariance(residualSecondMomentAR, Some("Monovariate residual covariance"), Some("Monovariate_res_covariance_taxis.png"))
    PlotTS.showCovariance(residualSecondMomentVAR, Some("Multivariate residual covariance"), Some("Multivariate_res_covariance_taxis.png"))

    println("Frequentist VAR residuals")
    println(trace(residualSecondMomentVAR))
    println()



  }
}