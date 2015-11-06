package main.scala.overlapping.timeSeries.secondOrder.multivariate.bayesianEstimators.gradients

import breeze.linalg.{diag, DenseMatrix, DenseVector}
import org.apache.spark.broadcast.Broadcast

/**
 * Created by Francois Belletti on 9/28/15.
 */
class DiagonalNoiseMAGrad[IndexT <: Ordered[IndexT]](
   val sigmaEps: DenseVector[Double],
   val nSamples: Long,
   val mean: Broadcast[DenseVector[Double]])
  extends Serializable{

  val d = sigmaEps.size
  val precisionMatrix = DenseVector.ones[Double](d)
  precisionMatrix :/= sigmaEps

  val precisionMatrixAsDiag = diag(precisionMatrix)

  def apply(params: Array[DenseMatrix[Double]],
            data: Array[(IndexT, DenseVector[Double])],
            innovations: Array[DenseVector[Double]]): (Array[DenseMatrix[Double]], Array[DenseVector[Double]]) = {

    val q = params.length
    val totGradient   = Array.fill(q){DenseMatrix.zeros[Double](d, d)}
    val prevision     = DenseVector.zeros[Double](d)

    val meanValue = mean.value

    for(h <- 1 to q){
      prevision += params(h - 1) * innovations(h - 1)
    }

    val newInnovation = data(0)._2 - meanValue - prevision

    val normError = precisionMatrixAsDiag * newInnovation

    for(h <- 1 to q){
      totGradient(h - 1) :-= normError * innovations(h - 1).t * 2.0 / nSamples.toDouble
    }

    (totGradient, Array(newInnovation) ++ innovations.slice(0, innovations.length - 1))

  }

}
