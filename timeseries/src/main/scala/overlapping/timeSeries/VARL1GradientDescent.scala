package main.scala.overlapping.timeSeries

import breeze.linalg._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import main.scala.overlapping.containers.SingleAxisBlock
import main.scala.overlapping.timeSeries.secondOrder.multivariate.bayesianEstimators.gradients.DiagonalNoiseARGrad
import main.scala.overlapping.timeSeries.secondOrder.multivariate.lossFunctions.DiagonalNoiseARLoss
import main.scala.overlapping.timeSeries.secondOrder.multivariate.bayesianEstimators.procedures.{GradientDescent, L1ClippedGradientDescent}

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 9/16/15.
 */
object VARL1GradientDescent{

  def apply[IndexT <: Ordered[IndexT] : ClassTag](
      timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])],
      p: Int,
      lambda: Double,
      precision: Double = 1e-6,
      maxIter: Int = 1000)
      (implicit config: TSConfig): Array[DenseMatrix[Double]] = {

    implicit val sc = timeSeries.context
    val estimator = new VARL1GradientDescent[IndexT](p, lambda, precision, maxIter)
    estimator.estimate(timeSeries)

  }

}


class VARL1GradientDescent[IndexT <: Ordered[IndexT] : ClassTag](
    p: Int,
    lambda: Double,
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

    val meanEstimator = new MeanEstimator[IndexT]()
    val mean = meanEstimator.estimate(timeSeries)

    val freqVAREstimator = new VARModel[IndexT](p, Some(mean))
    val (freqVARMatrices, _) = freqVAREstimator.estimate(timeSeries)

    val predictorVAR = new VARPredictor[IndexT](freqVARMatrices, Some(mean))
    val residualsVAR = predictorVAR.estimateResiduals(timeSeries)

    val secondMomentEstimator = new SecondMomentEstimator[IndexT]()
    val residualSecondMomentVAR = secondMomentEstimator.estimate(residualsVAR)
    val sigmaEpsilon = diag(residualSecondMomentVAR)

    val covEstimator = new Covariance[IndexT](Some(mean))
    val covMatrix = covEstimator.estimate(timeSeries)
    val svd.SVD(_, s, _) = svd(covMatrix)
    def stepSize(x: Int): Double ={
      1.0 / (max(s) * max(sigmaEpsilon) + min(s) * min(sigmaEpsilon))
    }

    val VARLoss = new DiagonalNoiseARLoss[IndexT](sigmaEpsilon, N, sc.broadcast(mean))
    val VARGrad = new DiagonalNoiseARGrad[IndexT](sigmaEpsilon, N, sc.broadcast(mean))

    val kernelizedLoss = new AutoregressiveLoss[IndexT](p, VARLoss.apply)
    val kernelizedGrad = new AutoregressiveGradient[IndexT](p, VARGrad.apply)

    val gradSizes = kernelizedGrad.getGradientSize

    L1ClippedGradientDescent.run[RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]](
      {case (param, data) => kernelizedLoss.setNewX(param); kernelizedLoss.timeSeriesStats(data)},
      {case (param, data) => kernelizedGrad.setNewX(param); kernelizedGrad.timeSeriesStats(data)},
      gradSizes,
      stepSize,
      precision,
      lambda,
      maxIter,
      freqVARMatrices,
      timeSeries
    )

  }

}
