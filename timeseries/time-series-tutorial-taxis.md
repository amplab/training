/*
  Compile by typing in this folder: sbt assembly
  Run by typing in this folder: sbt console
*/

/*
In this tutorial we are going to practice on actual data
and calibrate a multivariate autoregressive model.
*/

// Let's first load the data. This is taxi earnings in New York
// grouped into spatial cells and 5 minute time buckets.
val inSampleFilePath = "./data/taxis.csv"
val (inSampleDataHD, _, nSamples) = ReadCsv.TS(inSampleFilePath)

// Here this is just a sub-selection of some spatial cells.
// You can try with more dimensions, things will just be longer.
val inSampleData = inSampleDataHD.mapValues(v => v(60 until 120))

// Let's specify the configuration of the time series
val d = 60
val deltaTMillis = 5 * 60L * 1000L // 5 minutes.
val paddingMillis  = deltaTMillis * 100L // 100 step overlap.
val nPartitions   = 8
implicit val config = TSConfig(deltaTMillis, d, nSamples, paddingMillis.toDouble)

// Let's see how many samples and dimensions we have.
println(nSamples + " samples")
println(d + " dimensions")
println()

// Let's gather the data with seasonality into overlapping blocks.
val (rawTimeSeries: RDD[(Int, SingleAxisBlock[TSInstant, DenseVector[Double]])], _) =
  SingleAxisBlockRDD((paddingMillis, paddingMillis), nPartitions, inSampleData)

// Let's get rid of seasonality
// We group all the data in the same 5 minute window of the week together.
def hashFunction(x: TSInstant): Int = {
    (x.timestamp.getDayOfWeek - 1) * 24 * 60 / 5 + (x.timestamp.getMinuteOfDay - 1) / 5
}

// If we want to visualize the seasonal profile...
val matrixMeanProfile = DenseMatrix.zeros[Double](7 * 24 * 12, d)
val meanProfile = MeanProfileEstimator(rawTimeSeries, hashFunction)
for(k <- meanProfile.keys){
  matrixMeanProfile(k, ::) := meanProfile(k).t
}
PlotTS.showProfile(matrixMeanProfile, Some("Weekly demand profile"), Some("Weekly_demand_profile.png"))

// Let's remove the seasonal component from the data
val noSeason = MeanProfileEstimator.removeSeason(inSampleData, hashFunction, meanProfile)

// And put it in overlaping blocks.
val (timeSeriesRDD: RDD[(Int, SingleAxisBlock[TSInstant, DenseVector[Double]])], _) =
  SingleAxisBlockRDD((paddingMillis, paddingMillis), nPartitions, noSeason)

// Now we take a look at the auto-correlation structure of the data.
val (correlations, _) = CrossCorrelation(timeSeriesRDD, 4)
PlotTS.showModel(correlations, Some("Cross correlation"), Some("Correlations_taxis.png"))

// And similarly with partial auto-correlation.
val (partialCorrelations, _) = PartialCrossCorrelation(timeSeriesRDD,4)
PlotTS.showModel(partialCorrelations, Some("Partial cross correlation"), Some("Partial_correlation_taxis.png"))

// We'll try and calibrate an AR(3) model on that data.
val chosenP = 3

// Let's first proceed with an univariate model.
val mean = MeanEstimator(timeSeriesRDD)
val vectorsAR = ARModel(timeSeriesRDD, chosenP, Some(mean)).map(_.covariation)

// Let's compute the residuals
val residualsAR = ARPredictor(timeSeriesRDD, vectorsAR, Some(mean))

// And their covariance matrix. As usual we want it to be a diagonal.
val residualSecondMomentAR = SecondMomentEstimator(residualsAR)

// Let's take a look at the coefficients of the model.
PlotTS.showUnivModel(vectorsAR, Some("Monovariate parameter estimates"), Some("Univariate_model_taxis.png"))

// Let's now estimate a multivariate model so as to take spatial
// inter-dependencies into account.
val (estVARMatrices, _) = VARModel(timeSeriesRDD, chosenP)

// Let's examine the coefficients of the models
PlotTS.showModel(estVARMatrices, Some("Multivariate parameter estimates"), Some("VAR_model_taxis.png"))

// compute the residuals
val residualVAR = VARPredictor(timeSeriesRDD, estVARMatrices, Some(mean))

// and their variance-covariance matrix. Again we want to avoid extra-diagonal terms.
val residualSecondMomentVAR = SecondMomentEstimator(residualVAR)
PlotTS.showCovariance(residualSecondMomentAR, Some("Monovariate residual covariance"), Some("Monovariate_res_covariance_taxis.png"))
PlotTS.showCovariance(residualSecondMomentVAR, Some("Multivariate residual covariance"), Some("Multivariate_res_covariance_taxis.png"))

// Now let's take a look at the mean squared error of the residuals.
println("AR in sample error = " + trace(residualSecondMomentAR))
println("VAR in sample error = " + trace(residualSecondMomentVAR))
println()