/*

/**
 * Created by cusgadmin on 6/9/15.
 */

object RunWithSurrogateDataMultiBlock {

  def main(args: Array[String]): Unit ={

    /*

    val nColumns      = 4
    val nSamples      = 1000000L
    val paddingMillis = 1000L
    val deltaTMillis  = 1L
    val nPartitions   = 8

    val conf  = new SparkConf().setAppName("Counter").setMaster("local[*]")
    val sc    = new SparkContext(conf)

    val ARcoeffs = Array(DenseMatrix((0.15, 0.10, 0.0, 0.0),
      (0.10, 0.15, 0.10, 0.0),
      (0.0, 0.10, 0.15, 0.10),
      (0.0, 0.0, 0.10, 0.15)))

    val lateralPartitions     = Array(Array(0, 1, 2), Array(1, 2, 3))
    val lateralPartitionRows  = Array(Array(0, 1), Array(2, 3))
    val originalLateralRows   =  Array(Array(0, 1), Array(1, 2))

    val rawTS = IndividualRecords.generateVAR(
      ARcoeffs,
      nColumns, nSamples.toInt, deltaTMillis,
      Gaussian(0.0, 1.0),
      sc)

    implicit val DateTimeOrdering = new Ordering[(DateTime, Array[Double])] {
      override def compare(a: (DateTime, Array[Double]), b: (DateTime, Array[Double])) =
        a._1.compareTo(b._1)
    }

    val signedDistance = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

    val (overlappingRDD: RDD[(Int, SingleAxisBlock[TSInstant, DenseVector[Double]])], intervals: Array[(TSInstant, TSInstant)]) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), signedDistance, nPartitions, rawTS)

    val (splitOverlappingRDD: RDD[(Int, SingleAxisBlock[TSInstant, DenseVector[Double]])], splitIntervals: Array[(TSInstant, TSInstant)]) =
      SingleAxisBlockRDD.splitDenseVector(
        (paddingMillis, paddingMillis),
        signedDistance,
        nPartitions,
        lateralPartitions,
        rawTS)

    val p = ARcoeffs.length

    println("Results of cross cov frequentist estimator")

    val crossCovEstimator = new CrossCovariance[TSInstant](1.0, 3, nColumns, DenseVector.zeros(nColumns))
    val (crossCovMatrices, covMatrix) = crossCovEstimator
      .estimate(overlappingRDD)

    crossCovMatrices.foreach(x=> {println(x); println()})

    val svd.SVD(_, s, _) = svd(covMatrix)

    val gradientSizes = Array(Array((2, 3)), Array((2, 3)))
    val batchSize = nSamples

    def lossFunctionAR(d: Int, latRows: Array[Int], originalLatRows: Array[Int])
                      (params: Array[DenseMatrix[Double]],
                       data: Array[(TSInstant, DenseVector[Double])]): Double = {
      var totError = 0.0
      val prevision = DenseVector.zeros[Double](d)
      val error     = DenseVector.zeros[Double](d)
      for(i <- p until data.length){
        prevision := 0.0
        for(h <- 1 to p){
          prevision += params(h - 1) * data(i - h)._2
        }
        error := DenseVector(originalLatRows.map(h => data(i)._2(h))) - prevision
        totError += sum(error :* error)
      }
      totError / batchSize.toDouble
    }

    val lossFunctions = lateralPartitions
      .indices
      .map(i => lossFunctionAR(gradientSizes(i)(0)._1, lateralPartitionRows(i), originalLateralRows(i)) _)
      .toArray

    def gradientFunctionAR(m: Int, n: Int, latRows: Array[Int], originalLatRows: Array[Int])
                          (params: Array[DenseMatrix[Double]],
                           data: Array[(TSInstant, DenseVector[Double])]): Array[DenseMatrix[Double]] = {
      val totGradient   = Array.fill(p){DenseMatrix.zeros[Double](m, n)}
      val prevision     = DenseVector.zeros[Double](m)
      for(i <- p until data.length){
        prevision := 0.0
        for(h <- 1 to p){
          prevision += params(h - 1) * data(i - h)._2
        }
        val dataRef = DenseVector(originalLatRows.map(h => data(i)._2(h)))
        for(h <- 1 to p){
          totGradient(h - 1) :-= (dataRef - prevision) * data(i - h)._2.t
        }
      }
      for(h <- 1 to p) {
        totGradient(h - 1) :*= 2.0 / batchSize.toDouble
      }
      totGradient
    }

    val gradientFunctions = lateralPartitions
      .indices
      .map(i => gradientFunctionAR(gradientSizes(i)(0)._1, gradientSizes(i)(0)._2, lateralPartitionRows(i), originalLateralRows(i)) _)
      .toArray

    /*
    The Hessian to the AR calibration is exactly the variance covariance matrix
     */
    def stepSize(x: Int): Double ={
      1.0 / (max(s) + min(s))
      /*
      val m = min(s)
      val L = 2 * max(s)
      1.0 / (2.0 * m * (0.5 * L * L / (m * m) + x))
      */
      //
    }

    println("Results of AR multivariate bayesian estimator")

    val VARBayesEstimator = new LateralSplitVARGradientDescent[TSInstant](
      p,
      deltaTMillis,
      lateralPartitions,
      lateralPartitions.length,
      lossFunctions,
      gradientFunctions,
      gradientSizes,
      stepSize,
      1e-5,
      100,
      gradientSizes.map(x => Array.fill(p){DenseMatrix.zeros[Double](x(0)._1, x(0)._2)})
    )

    val ARMatrices = VARBayesEstimator.estimate(splitOverlappingRDD)

    println("AR multivaraite bayesian model:")
    ARMatrices.foreach(x=> {println(x); println()})

    println()

    */

  }
}

*/