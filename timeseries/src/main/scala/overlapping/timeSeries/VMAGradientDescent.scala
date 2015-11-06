package main.scala.overlapping.timeSeries

import breeze.linalg._
import breeze.numerics.abs
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import main.scala.overlapping.containers.SingleAxisBlock
import main.scala.overlapping.timeSeries.secondOrder.multivariate.bayesianEstimators.gradients.{DiagonalNoiseMAGrad, DiagonalNoiseARGrad}
import main.scala.overlapping.timeSeries.secondOrder.multivariate.bayesianEstimators.lossFunctions.DiagonalNoiseMALoss
import main.scala.overlapping.timeSeries.secondOrder.multivariate.bayesianEstimators.procedures.GradientDescent
import main.scala.overlapping.timeSeries.secondOrder.multivariate.lossFunctions.DiagonalNoiseARLoss

import scala.reflect.ClassTag


object VMAGradientDescent{

  def apply[IndexT <: Ordered[IndexT] : ClassTag](
      timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])],
      p: Int,
      precision: Double = 1e-4,
      maxIter: Int = 100)
      (implicit config: TSConfig): Array[DenseMatrix[Double]] = {

    implicit val sc = timeSeries.context
    val estimator = new VMAGradientDescent[IndexT](p, precision, maxIter)
    estimator.estimate(timeSeries)

  }

}

class VMAGradientDescent[IndexT <: Ordered[IndexT] : ClassTag](
    q: Int,
    precision: Double = 1e-4,
    maxIter: Int = 1000)
    (implicit config: TSConfig, sc: SparkContext)
  extends Estimator[IndexT, DenseVector[Double], Array[DenseMatrix[Double]]]{

  val d = config.d
  val N = config.nSamples
  val deltaT = config.deltaT

  if(deltaT * q > config.padding){
    throw new IndexOutOfBoundsException("Not enough padding to support model estimation.")
  }

  override def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]): Array[DenseMatrix[Double]] = {

    val mean = MeanEstimator(timeSeries)

    val (freqVMAMatrices, noiseVariance) = VMAModel(timeSeries, q, Some(mean))

    /*
    val predictorVAR = new VARPredictor[IndexT](freqVARMatrices, Some(mean))
    val residualsVAR = predictorVAR.estimateResiduals(timeSeries)

    val secondMomentEstimator = new SecondMomentEstimator[IndexT]()
    val residualSecondMomentVAR = secondMomentEstimator.estimate(residualsVAR)
    val sigmaEpsilon = diag(residualSecondMomentVAR)
    */

    val sigmaEpsilon = diag(noiseVariance)

    /*
    Redundant computation of cross-cov Matrix, need to do something about that
     */
    val (crossCovMatrices, _) = CrossCovariance(timeSeries, q, Some(mean))

    val allEigenValues = crossCovMatrices.map(x => abs(eig(x).eigenvalues))
    val maxEig = max(allEigenValues.map(x => max(x)))
    val minEig = min(allEigenValues.map(x => min(x)))

    def stepSize(x: Int): Double ={
      0.9 / (maxEig * max(sigmaEpsilon) + minEig * min(sigmaEpsilon))
    }

    val VMALoss = new DiagonalNoiseMALoss[IndexT](sigmaEpsilon, N, sc.broadcast(mean))
    val VMAGrad = new DiagonalNoiseMAGrad[IndexT](sigmaEpsilon, N, sc.broadcast(mean))

    val kernelizedLoss = new MemoryLoss[IndexT](q, VMALoss.apply)
    val kernelizedGrad = new MemoryGradient[IndexT](q, VMAGrad.apply)

    val gradSizes = kernelizedGrad.getGradientSize

    GradientDescent.run[RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]](
      {case (param, data) => kernelizedLoss.setNewX(param); kernelizedLoss.timeSeriesStats(data)},
      {case (param, data) => kernelizedGrad.setNewX(param); kernelizedGrad.timeSeriesStats(data)},
      gradSizes,
      stepSize,
      precision,
      maxIter,
      freqVMAMatrices,
      timeSeries
    )

  }

}
