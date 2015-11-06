# Time series analysis on taxi data in New York.

## Compilation notes:
Compile by typing in time series folder: sbt assembly .
Run by typing in this folder: sbt console .

In this tutorial we are going to practice on actual data
and calibrate a multivariate autoregressive model.

## Loading the data

1. Let's first load the data. This is taxi earnings in New York
grouped into spatial cells and 5 minute time buckets.
<div class="codetabs">
<div data-lang="scala" markdown="1">
<pre class="prettyprint lang-bsh">
val inSampleFilePath = "./data/taxis.csv"
val (inSampleDataHD, _, nSamples) = ReadCsv.TS(inSampleFilePath)
</pre>
</div>
</div>

2. Here this is just a sub-selection of some spatial cells.
You can try with more dimensions, things will just be longer.
<div class="codetabs">
<div data-lang="scala" markdown="1">
<pre class="prettyprint lang-bsh">
val inSampleData = inSampleDataHD.mapValues(v => v(60 until 120))
</pre>
</div>
</div>

## Configuring the time series

1. Let's specify the configuration of the time series
<div class="codetabs">
<div data-lang="scala" markdown="1">
<pre class="prettyprint lang-bsh">
val d = 60
val deltaTMillis = 5 * 60L * 1000L // 5 minutes.
val paddingMillis  = deltaTMillis * 100L // 100 step overlap.
val nPartitions   = 8
implicit val config = TSConfig(deltaTMillis, d, nSamples, paddingMillis.toDouble)
</pre>
</div>
</div>

2. Let's see how many samples and dimensions we have.
<div class="codetabs">
<div data-lang="scala" markdown="1">
<pre class="prettyprint lang-bsh">
println(nSamples + " samples")
println(d + " dimensions")
println()
</pre>
</div>
</div>

3. Let's gather the data with seasonality into overlapping blocks.
<div class="codetabs">
<div data-lang="scala" markdown="1">
<pre class="prettyprint lang-bsh">
val (rawTimeSeries: RDD[(Int, SingleAxisBlock[TSInstant, DenseVector[Double]])], _) =
  SingleAxisBlockRDD((paddingMillis, paddingMillis), nPartitions, inSampleData)
</pre>
</div>
</div>

## Identifying seasonality.

4. Let's get rid of seasonality. We group all the data in the same 5 minute window of the week together.
<div class="codetabs">
<div data-lang="scala" markdown="1">
<pre class="prettyprint lang-bsh">
def hashFunction(x: TSInstant): Int = {
    (x.timestamp.getDayOfWeek - 1) * 24 * 60 / 5 + (x.timestamp.getMinuteOfDay - 1) / 5
}
</pre>
</div>
</div>

5. If we want to visualize the seasonal profile.
<div class="codetabs">
<div data-lang="scala" markdown="1">
<pre class="prettyprint lang-bsh">
val matrixMeanProfile = DenseMatrix.zeros[Double](7 * 24 * 12, d)
val meanProfile = MeanProfileEstimator(rawTimeSeries, hashFunction)
for(k <- meanProfile.keys){
  matrixMeanProfile(k, ::) := meanProfile(k).t
}
PlotTS.showProfile(matrixMeanProfile, Some("Weekly demand profile"), Some("Weekly_demand_profile.png"))
</pre>
</div>
</div>

6.  Let's remove the seasonal component from the data and put it in overlaping blocks.
<div class="codetabs">
<div data-lang="scala" markdown="1">
<pre class="prettyprint lang-bsh">
val noSeason = MeanProfileEstimator.removeSeason(inSampleData, hashFunction, meanProfile)
val (timeSeriesRDD: RDD[(Int, SingleAxisBlock[TSInstant, DenseVector[Double]])], _) =
    SingleAxisBlockRDD((paddingMillis, paddingMillis), nPartitions, noSeason)
</pre>
</div>
</div>

## Analysis of the data without seasonality

1. Now we take a look at the auto-correlation structure of the data.
<div class="codetabs">
<div data-lang="scala" markdown="1">
<pre class="prettyprint lang-bsh">
val (correlations, _) = CrossCorrelation(timeSeriesRDD, 4)
PlotTS.showModel(correlations, Some("Cross correlation"), Some("Correlations_taxis.png"))
</pre>
</div>
</div>

2. And proceed similarly with partial auto-correlation.
<div class="codetabs">
<div data-lang="scala" markdown="1">
<pre class="prettyprint lang-bsh">
val (partialCorrelations, _) = PartialCrossCorrelation(timeSeriesRDD,4)
PlotTS.showModel(partialCorrelations, Some("Partial cross correlation"), Some("Partial_correlation_taxis.png"))
</pre>
</div>
</div>

## Autoregressive model estimation

We'll try and calibrate an AR(3) model on that data.
val chosenP = 3

1. Let's first proceed with an univariate model.
<div class="codetabs">
<div data-lang="scala" markdown="1">
<pre class="prettyprint lang-bsh">
val mean = MeanEstimator(timeSeriesRDD)
val vectorsAR = ARModel(timeSeriesRDD, chosenP, Some(mean)).map(_.covariation)
</pre>
</div>
</div>

2. Let's compute the residuals
<div class="codetabs">
<div data-lang="scala" markdown="1">
<pre class="prettyprint lang-bsh">
val residualsAR = ARPredictor(timeSeriesRDD, vectorsAR, Some(mean))
</pre>
</div>
</div>

3. And their covariance matrix. As usual we want it to be a diagonal.
<div class="codetabs">
<div data-lang="scala" markdown="1">
<pre class="prettyprint lang-bsh">
val residualSecondMomentAR = SecondMomentEstimator(residualsAR)
</pre>
</div>
</div>

4. Let's take a look at the coefficients of the model.
<div class="codetabs">
<div data-lang="scala" markdown="1">
<pre class="prettyprint lang-bsh">
PlotTS.showUnivModel(vectorsAR, Some("Monovariate parameter estimates"), Some("Univariate_model_taxis.png"))
</pre>
</div>
</div>

5. Let's now estimate a multivariate model so as to take spatial inter-dependencies into account.
<div class="codetabs">
<div data-lang="scala" markdown="1">
<pre class="prettyprint lang-bsh">
val (estVARMatrices, _) = VARModel(timeSeriesRDD, chosenP)
</pre>
</div>
</div>

6. Let's examine the coefficients of the models.
<div class="codetabs">
<div data-lang="scala" markdown="1">
<pre class="prettyprint lang-bsh">
PlotTS.showModel(estVARMatrices, Some("Multivariate parameter estimates"), Some("VAR_model_taxis.png"))
</pre>
</div>
</div>

7. Let's compute the residuals.
<div class="codetabs">
<div data-lang="scala" markdown="1">
<pre class="prettyprint lang-bsh">
val residualVAR = VARPredictor(timeSeriesRDD, estVARMatrices, Some(mean))
</pre>
</div>
</div>

8. and their variance-covariance matrix. Again we want to avoid extra-diagonal terms.
<div class="codetabs">
<div data-lang="scala" markdown="1">
<pre class="prettyprint lang-bsh">
val residualSecondMomentVAR = SecondMomentEstimator(residualVAR)
PlotTS.showCovariance(residualSecondMomentAR, Some("Monovariate residual covariance"), Some("Monovariate_res_covariance_taxis.png"))
PlotTS.showCovariance(residualSecondMomentVAR, Some("Multivariate residual covariance"), Some("Multivariate_res_covariance_taxis.png"))
</pre>
</div>
</div>

9. Now let's take a look at the mean squared error of the residuals.
<div class="codetabs">
<div data-lang="scala" markdown="1">
<pre class="prettyprint lang-bsh">
println("AR in sample error = " + trace(residualSecondMomentAR))
println("VAR in sample error = " + trace(residualSecondMomentVAR))
println()
</pre>
</div>
</div>