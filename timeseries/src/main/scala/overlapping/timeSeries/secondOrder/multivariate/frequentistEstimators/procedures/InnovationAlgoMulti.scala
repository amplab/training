package main.scala.overlapping.timeSeries.secondOrder.multivariate.frequentistEstimators.procedures

import breeze.linalg._

/**
 * Created by Francois Belletti on 7/14/15.
 */

object InnovationAlgoMulti extends Serializable{

  /*

  Multivariate version of the innovation algorithm.

  Expects autocovariations of negative rank (-modelOrder ... modelOrder)

  TODO: shield procedure against the following edge cases, autoCov.size < 1, autoCov(0) = 0.0
   */
  def apply(q: Int, crossCovMatrices: Array[DenseMatrix[Double]]): (Array[DenseMatrix[Double]], DenseMatrix[Double]) ={
    val d = crossCovMatrices(0).rows

    val thetaEsts   = (1 to q).toArray.map(Array.fill(_){DenseMatrix.zeros[Double](d, d)})
    val varEsts     = Array.fill(q + 1){DenseMatrix.zeros[Double](d, d)}
    val invVarEsts  = Array.fill(q + 1){DenseMatrix.zeros[Double](d, d)}

    varEsts(0)      = crossCovMatrices(q)
    invVarEsts(0)   = inv(varEsts(0))

    for(m <- 1 to q){
      for(j <- 0 until m){
        thetaEsts(m - 1)(m - 1 - j) := crossCovMatrices(j - m + q)
        for(i <- 0 until j){
          thetaEsts(m - 1)(m - 1 - j) += - thetaEsts(m - 1)(m - 1 - i) * varEsts(i) * thetaEsts(j)(j - 1 - i).t
        }
        thetaEsts(m - 1)(m - 1 - j) = thetaEsts(m - 1)(m - 1 - j) * invVarEsts(j)
      }
      varEsts(m) = crossCovMatrices(q)
      for(i <- 0 until m){
        varEsts(m) += - thetaEsts(m - 1)(i) * varEsts(i) * thetaEsts(m - 1)(m - 1 - i)
      }
      invVarEsts(m) = inv(varEsts(m))
    }

    (thetaEsts(q - 1), varEsts(q))

  }

}
