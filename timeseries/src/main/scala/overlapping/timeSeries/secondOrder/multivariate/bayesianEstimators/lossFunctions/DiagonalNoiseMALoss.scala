package main.scala.overlapping.timeSeries.secondOrder.multivariate.bayesianEstimators.lossFunctions

import breeze.linalg.{diag, DenseMatrix, DenseVector}
import org.apache.spark.broadcast.Broadcast

/**
 * Created by Francois Belletti on 9/28/15.
 */
class DiagonalNoiseMALoss[IndexT <: Ordered[IndexT]](
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
            innovations: Array[DenseVector[Double]]): (Double, Array[DenseVector[Double]]) = {

    val q = params.length
    val prevision     = DenseVector.zeros[Double](d)

    val meanValue = mean.value

    for(h <- 1 to q){
      prevision += params(h - 1) * innovations(h - 1)
    }

    val newInnovation = data(0)._2 - meanValue - prevision

    ((newInnovation dot (precisionMatrixAsDiag * newInnovation)) / nSamples.toDouble,
      Array(newInnovation) ++ innovations.slice(0, innovations.length - 1))

  }

}
