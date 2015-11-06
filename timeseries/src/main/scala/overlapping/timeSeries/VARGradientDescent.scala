package main.scala.overlapping.timeSeries

import breeze.linalg._
import breeze.numerics.abs
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel._
import main.scala.overlapping.containers.SingleAxisBlock
import main.scala.overlapping.timeSeries.secondOrder.multivariate.bayesianEstimators.gradients.DiagonalNoiseARGrad
import main.scala.overlapping.timeSeries.secondOrder.multivariate.bayesianEstimators.procedures.GradientDescent
import main.scala.overlapping.timeSeries.secondOrder.multivariate.lossFunctions.DiagonalNoiseARLoss

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 9/16/15.
 */

object VARGradientDescent{

  def apply[IndexT <: Ordered[IndexT] : ClassTag](
      timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])],
      p: Int,
      precision: Double = 1e-4,
      maxIter: Int = 100)
      (implicit config: TSConfig): Array[DenseMatrix[Double]] = {

      implicit val sc = timeSeries.context
      val estimator = new VARGradientDescent[IndexT](p, precision, maxIter)
      estimator.estimate(timeSeries)

  }

}


class VARGradientDescent[IndexT <: Ordered[IndexT] : ClassTag](
    p: Int,
    precision: Double = 1e-6,
    maxIter: Int = 1000)
    (implicit config: TSConfig, sc: SparkContext)
  extends Estimator[IndexT, DenseVector[Double], Array[DenseMatrix[Double]]]{

  val d = config.d
  val N = config.nSamples
  val deltaT = config.deltaT

  if(deltaT * p > config.padding){
    throw new IndexOutOfBoundsException("Not enough padding to support model estimation.")
  }

  override def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]): Array[DenseMatrix[Double]] = {

    val mean = MeanEstimator(timeSeries)

    val (freqVARMatrices, noiseVariance) = VARModel(timeSeries, p, Some(mean))

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
    val (crossCovMatrices, _) = CrossCovariance(timeSeries, p, Some(mean))

    val allEigenValues = crossCovMatrices.map(x => abs(eig(x).eigenvalues))
    val maxEig = max(allEigenValues.map(x => max(x)))
    val minEig = min(allEigenValues.map(x => min(x)))

    def stepSize(x: Int): Double ={
      2.0 / (maxEig * max(sigmaEpsilon) + minEig * min(sigmaEpsilon))
    }

    val VARLoss = new DiagonalNoiseARLoss[IndexT](sigmaEpsilon, N, sc.broadcast(mean))
    val VARGrad = new DiagonalNoiseARGrad[IndexT](sigmaEpsilon, N, sc.broadcast(mean))

    val kernelizedLoss = new AutoregressiveLoss[IndexT](p, VARLoss.apply)
    val kernelizedGrad = new AutoregressiveGradient[IndexT](p, VARGrad.apply)

    val gradSizes = kernelizedGrad.getGradientSize

    GradientDescent.run[RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]](
      {case (param, data) => kernelizedLoss.setNewX(param); kernelizedLoss.timeSeriesStats(data)},
      {case (param, data) => kernelizedGrad.setNewX(param); kernelizedGrad.timeSeriesStats(data)},
      gradSizes,
      stepSize,
      precision,
      maxIter,
      freqVARMatrices,
      timeSeries
    )

  }

}
