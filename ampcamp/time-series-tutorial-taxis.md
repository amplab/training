---
layout: global
title: Time Series Analysis on Taxi Data in New York
categories: [module]
navigation:
  weight: 100
  show: true
---

{:toc}

# Time Series Analysis on Taxi Data in New York

https://github.com/bellettif/sparkGeoTS

## Getting Started
In the shell, from the usb/spark/, please enter

<pre class="prettyprint lang-bsh">
usb/$ ./spark/bin/spark-shell --master "local[4]" --jars timeseries/sparkgeots.jar --driver-memory 2G
</pre>

and then please copy and paste the following in the Spark shell:

<div class="codetabs">
<div data-lang="scala" markdown="1">
    import breeze.linalg._
    import breeze.stats.distributions.Gaussian
    import breeze.numerics.sqrt
    import org.apache.spark.rdd.RDD
    import org.apache.spark.{SparkConf, SparkContext}
    import main.scala.overlapping._
    import containers._
    import timeSeries._
    import main.scala.ioTools.ReadCsv

    implicit def signedDistMillis = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble
</div>
</div>


In this tutorial we are going to practice on artificially generated data
and show that we are able to successfully identify autoregressive models.
In this tutorial we are going to practice on actual data
and calibrate a multivariate autoregressive model.

## Loading the Data

Let's first load the data. This is taxi earnings in New York
grouped into spatial cells and 5 minute time buckets.

<div class="codetabs">
<div data-lang="scala" markdown="1">
    val inSampleFilePath = "timeseries/taxis.csv"
    val (inSampleDataHD, _, nSamples) = ReadCsv.TS(inSampleFilePath)(sc)
</div>
</div>

Here this is just a sub-selection of some spatial cells.
You can try with more dimensions, things will just be longer.

<div class="codetabs">
<div data-lang="scala" markdown="1">
    val inSampleData = inSampleDataHD.mapValues(v => v(60 until 120))
</div>
</div>

## Configuring the Time Series

Let's specify the configuration of the time series

<div class="codetabs">
<div data-lang="scala" markdown="1">
    val d = 60
    val deltaTMillis = 5 * 60L * 1000L // 5 minutes.
    val paddingMillis  = deltaTMillis * 100L // 100 step overlap.
    val nPartitions   = 8
    implicit val config = TSConfig(deltaTMillis, d, nSamples, paddingMillis.toDouble)
</div>
</div>

Let's have an overlap between partitions of 100 ms.
The data in our partitions will overlap as follows:

    ------------------------------------
                                    -----------------------------------
                                                                  ---------------------------------

This is necessary to estimate our models without shuffling data between nodes.
With this setup, we will be able to calibrate models of any order lower that 100.

Let's see how many samples and dimensions we have.

<div class="codetabs">
<div data-lang="scala" markdown="1">
    println(nSamples + " samples")
    println(d + " dimensions")
    println()
</div>
</div>

Let's gather the data with seasonality into overlapping blocks.

<div class="codetabs">
<div data-lang="scala" markdown="1">
    val (rawTimeSeries: RDD[(Int, SingleAxisBlock[TSInstant, DenseVector[Double]])], _) =
        SingleAxisBlockRDD((paddingMillis, paddingMillis), nPartitions, inSampleData)
    PlotTS(rawTimeSeries, Some("In Sample Data"), Some(Array(15, 30, 45)))
</div>
</div>

We can see on the plot that has been generated for three regions that there is strong seasonal component to the earnings of taxis.


## Identifying Seasonality

Let's get rid of seasonality. We group all the data in the same 5 minute window of the week together.

<div class="codetabs">
<div data-lang="scala" markdown="1">
    def hashFunction(x: TSInstant): Int = {
        (x.timestamp.getDayOfWeek - 1) * 24 * 60 / 5 + (x.timestamp.getMinuteOfDay - 1) / 5
    }
</div>
</div>

If we want to visualize the seasonal profile.

<div class="codetabs">
<div data-lang="scala" markdown="1">
    val matrixMeanProfile = DenseMatrix.zeros[Double](7 * 24 * 12, d)
    val meanProfile = MeanProfileEstimator(rawTimeSeries, hashFunction)
    for(k <- meanProfile.keys){
        matrixMeanProfile(k, ::) := meanProfile(k).t
    }
    PlotTS.showProfile(matrixMeanProfile, Some("Weekly demand profile"), Some("Weekly_demand_profile.png"))
</div>
</div>

Let's remove the seasonal component from the data and put it in overlaping blocks.

<div class="codetabs">
<div data-lang="scala" markdown="1">
    val noSeason = MeanProfileEstimator.removeSeason(inSampleData, hashFunction, meanProfile)
    val (timeSeriesRDD: RDD[(Int, SingleAxisBlock[TSInstant, DenseVector[Double]])], _) =
    SingleAxisBlockRDD((paddingMillis, paddingMillis), nPartitions, noSeason)
</div>
</div>

## Analysis of the Data Without Seasonality

Now we take a look at the auto-correlation structure of the data.

<div class="codetabs">
<div data-lang="scala" markdown="1">
    val (correlations, _) = CrossCorrelation(timeSeriesRDD, 4)
    PlotTS.showModel(correlations, Some("Cross correlation"), Some("Correlations_taxis.png"))
</div>
</div>

And proceed similarly with partial auto-correlation.

<div class="codetabs">
<div data-lang="scala" markdown="1">
    val (partialCorrelations, _) = PartialCrossCorrelation(timeSeriesRDD,4)
    PlotTS.showModel(partialCorrelations, Some("Partial cross correlation"), Some("Partial_correlation_taxis.png"))
</div>
</div>

## Autoregressive Model Estimation

We'll try and calibrate an AR(3) model on that data.

<div class="codetabs">
<div data-lang="scala" markdown="1">
    val chosenP = 3
</div>
</div>

Let's first proceed with an univariate model.

<div class="codetabs">
<div data-lang="scala" markdown="1">
    val mean = MeanEstimator(timeSeriesRDD)
    val vectorsAR = ARModel(timeSeriesRDD, chosenP, Some(mean)).map(_.covariation)
</div>
</div>

Let's compute the residuals...

<div class="codetabs">
<div data-lang="scala" markdown="1">
    val residualsAR = ARPredictor(timeSeriesRDD, vectorsAR, Some(mean))
</div>
</div>

and their covariance matrix. As usual we want it to be a diagonal.

<div class="codetabs">
<div data-lang="scala" markdown="1">
    val residualSecondMomentAR = SecondMomentEstimator(residualsAR)
</div>
</div>

Let's take a look at the coefficients of the model.

<div class="codetabs">
<div data-lang="scala" markdown="1">
    PlotTS.showUnivModel(vectorsAR, Some("Monovariate parameter estimates"), Some("Univariate_model_taxis.png"))
</div>
</div>

Let's now estimate a multivariate model so as to take spatial inter-dependencies into account.

<div class="codetabs">
<div data-lang="scala" markdown="1">
    val (estVARMatrices, _) = VARModel(timeSeriesRDD, chosenP)
</div>
</div>

Let's examine the coefficients of the models.

<div class="codetabs">
<div data-lang="scala" markdown="1">
    PlotTS.showModel(estVARMatrices, Some("Multivariate parameter estimates"), Some("VAR_model_taxis.png"))
</div>
</div>

Let's compute the residuals...

<div class="codetabs">
<div data-lang="scala" markdown="1">
    val residualVAR = VARPredictor(timeSeriesRDD, estVARMatrices, Some(mean))
</div>
</div>

and their variance-covariance matrix. Once again we want to avoid extra-diagonal terms.

<div class="codetabs">
<div data-lang="scala" markdown="1">
    val residualSecondMomentVAR = SecondMomentEstimator(residualVAR)
    PlotTS.showCovariance(residualSecondMomentAR, Some("Monovariate residual covariance"),  Some("Monovariate_res_covariance_taxis.png"))
    PlotTS.showCovariance(residualSecondMomentVAR, Some("Multivariate residual covariance"), Some("Multivariate_res_covariance_taxis.png"))
</div>
</div>

Now let's take a look at the mean squared error of the residuals. We compute the mean squared errors which sum all 
squared errors along the sensing dimensions. The averate error we make when predicting is therefore obtained 
by dividing by the number of sensors and taking the square root of the result.

<div class="codetabs">
<div data-lang="scala" markdown="1">
    println("AR in sample error = " + trace(residualSecondMomentAR))
    println("Monovariate average error magnitude = " + sqrt(trace(residualSecondMomentAR) / d))
    println("VAR in sample error = " + trace(residualSecondMomentVAR))
    println("Multivariate average error magnitude = " + sqrt(trace(residualSecondMomentVAR) / d))
    println()
</div>
</div>

We can see that we can improve the prediction of demand by a couple of cents per spatial unit and 
5 minute window if we use a multivariate model.
