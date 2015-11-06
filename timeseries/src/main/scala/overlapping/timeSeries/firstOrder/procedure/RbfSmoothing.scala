package main.scala.overlapping.timeSeries.firstOrder.procedure

import breeze.linalg.{DenseMatrix, max, min}
import breeze.numerics.exp

/**
 * Created by Francois Belletti on 9/29/15.
 */
object RbfSmoothing {

  def apply(input: DenseMatrix[Double],
            bandwidthX: Int,
            bandwidthY: Int,
            sigmaX: Double,
            sigmaY: Double): DenseMatrix[Double] = {

    val result = DenseMatrix.zeros[Double](input.rows, input.cols)

    var totWeight = 0.0

    for(i <- 0 until input.rows){
      for(j <- 0 until input.cols){

        totWeight = 0.0

        for(k <- max(0, i - bandwidthX) to min(input.rows - 1, i + bandwidthX)) {

          val weight = exp(- ((i - k) * (i - k)).toDouble / sigmaX)
          result(i, j) += weight * input(k, j)
          totWeight += weight

        }

        result(i ,j) /= totWeight

      }
    }


    for(i <- 0 until input.rows){
      for(j <- 0 until input.cols){

      totWeight = 0.0

      for(l <- max(0, j - bandwidthY) to min(input.cols - 1, j + bandwidthY)) {

        val weight = exp(- ((j - l) * (j - l)).toDouble / sigmaY)
        result(i, j) += weight * input(i, l)
        totWeight += weight

      }

      result(i ,j) /= totWeight

    }
  }

  result

  }

}
