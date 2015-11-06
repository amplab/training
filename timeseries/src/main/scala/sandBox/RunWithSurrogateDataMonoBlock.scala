/**
 * Created by cusgadmin on 6/9/15.
 */

/*

import breeze.linalg._
import breeze.numerics.sqrt
import breeze.stats.distributions.{Gaussian, Uniform}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import main.scala.overlapping.IntervalSize
import main.scala.overlapping.containers.block.{SingleAxisBlock, IntervalSampler}
import main.scala.overlapping.io.SingleAxisBlockRDD
import main.scala.overlapping.timeSeries.secondOrder._
import main.scala.overlapping.timeSeries.secondOrder.multivariate.bayesianEstimators.{VARGradientDescent, AutoregressiveGradient, AutoregressiveLoss, VARL1GradientDescent}
import main.scala.overlapping.timeSeries.secondOrder.multivariate.frequentistEstimators.{VARMAModel, VMAModel, VARModel, CrossCovariance}
import main.scala.overlapping.timeSeries.surrogateData.{TSInstant, IndividualRecords}

import scala.math.Ordering

object RunWithSurrogateDataMonoBlock {

  def main(args: Array[String]): Unit ={

    val nColumns      = 3
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
      nColumns, nSamples.toInt, deltaTMillis,
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

    val p = ARcoeffs.length

    println("Results of cross cov frequentist estimator")

    val crossCovEstimator = new CrossCovariance[TSInstant](1.0, p, nColumns, sc.broadcast(DenseVector.zeros(nColumns)))
    val (crossCovMatrices, covMatrix) = crossCovEstimator
      .estimate(overlappingRDD)

    crossCovMatrices.foreach(x=> {println(x); println()})

    val svd.SVD(_, s, _) = svd(covMatrix)

    println()

    println("Results of AR multivariate frequentist estimator")

    val VAREstimator = new VARModel[TSInstant](1.0, p, nColumns, sc.broadcast(DenseVector.zeros[Double](nColumns)))
    val (coeffMatricesAR, noiseVarianceAR) = VAREstimator
      .estimate(overlappingRDD)

    println("AR estimated model:")
    coeffMatricesAR.foreach(x=> {println(x); println()})
    println(noiseVarianceAR)

    println()

    println("Results of MA multivariate frequentist estimator")

    val VMAEstimator = new VMAModel[TSInstant](1.0, p, nColumns, sc.broadcast(DenseVector.zeros[Double](nColumns)))
    val (coeffMatricesMA, noiseVarianceMA) = VMAEstimator
      .estimate(overlappingRDD)

    println("MA estimated model:")
    coeffMatricesMA.foreach(x=> {println(x); println()})
    println(noiseVarianceMA)

    println()

    println("Results of ARMA multivariate frequentist estimator")

    val VARMAEstimator = new VARMAModel[TSInstant](1.0, p, p, nColumns, sc.broadcast(DenseVector.zeros[Double](nColumns)))
    val (coeffMatricesARMA, noiseVarianceARMA) = VARMAEstimator
      .estimate(overlappingRDD)

    println("ARMA estimated model:")
    coeffMatricesARMA.foreach(x=> {println(x); println()})
    println(noiseVarianceARMA)

    println()

    val gradientSizes = Array.fill(p){(2, 2)}

    def lossFunctionAR(params: Array[DenseMatrix[Double]],
                       data: Array[(TSInstant, DenseVector[Double])]): Double = {
      var totError = 0.0
      val prevision = DenseVector.zeros[Double](nColumns)
      val error     = DenseVector.zeros[Double](nColumns)
      for(i <- p until data.length){
        prevision := 0.0
        for(h <- 1 to p){
          prevision += params(h - 1) * data(i - h)._2
        }
        error := data(i)._2 - prevision
        totError += sum(error :* error)
      }
      totError / nSamples.toDouble
    }

    def gradientFunctionAR(params: Array[DenseMatrix[Double]],
                           data: Array[(TSInstant, DenseVector[Double])]): Array[DenseMatrix[Double]] = {
      val totGradient   = Array.fill(p){DenseMatrix.zeros[Double](nColumns, nColumns)}
      val prevision     = DenseVector.zeros[Double](nColumns)
      for(i <- p until data.length){
        prevision := 0.0
        for(h <- 1 to p){
          prevision += params(h - 1) * data(i - h)._2
        }
        for(h <- 1 to p){
          totGradient(h - 1) :-= (data(i)._2 - prevision) * data(i - h)._2.t
        }
      }
      for(h <- 1 to p) {
        totGradient(h - 1) :*= 2.0 / nSamples.toDouble
      }
      totGradient
    }

    def stepSize(x: Int): Double ={
      1.0 / (max(s) + min(s))
    }

    println("Results of AR multivariate bayesian estimator")

    val VARBayesEstimator = new VARGradientDescent[TSInstant](
      p,
      deltaTMillis,
      new AutoregressiveLoss(
        p,
        deltaTMillis,
        Array.fill(p){DenseMatrix.zeros[Double](nColumns, nColumns)},
        lossFunctionAR),
      new AutoregressiveGradient(
        p,
        deltaTMillis,
        Array.fill(p){DenseMatrix.zeros[Double](nColumns, nColumns)},
        gradientFunctionAR),
      stepSize,
      1e-5,
      1000,
      Array.fill(p){DenseMatrix.zeros(nColumns, nColumns)}
    )

    val ARMatrices = VARBayesEstimator.estimate(overlappingRDD)

    println("AR multivaraite bayesian model:")
    ARMatrices.foreach(x=> {println(x); println()})

    println()


  }
}

*/