package main.scala.overlapping.timeSeries.secondOrder.univariate.Procedures

import breeze.linalg._
import main.scala.overlapping.timeSeries._

/**
 * Created by Francois Belletti on 7/14/15.
 */

object InnovationAlgo extends Serializable{

  /*

  This calibrate one univariate AR model per columns.
  Returns an array of calibrated parameters (Coeffs, variance of noise).

  Check out Brockwell, Davis, Time Series: Theory and Methods, 1987 (p 238)
  TODO: shield procedure against the following edge cases, autoCov.size < 1, autoCov(0) = 0.0
   */
  def apply(q: Int, autoCov: DenseVector[Double]): SecondOrderSignature ={
    val thetaEsts = (1 to q).toArray.map(DenseVector.zeros[Double])
    val varEsts   = DenseVector.zeros[Double](q + 1)

    varEsts(0)    = autoCov(0) // Potential edge case here whenever varEsts(0) == 0

    for(m <- 1 to q){
      for(k <- 0 until m){
        // In the book the theta estimate vector is filled from the tail to the head.
        // Here it is filled from the head to the tail.
        thetaEsts(m - 1)(k) = (autoCov(m - k) - sum(thetaEsts(m - 1)(0 until k) :* varEsts(0 until k):* thetaEsts(k) )) / varEsts(k)
      }
      varEsts(m) = autoCov(0) - sum(thetaEsts(m - 1) :* thetaEsts(m - 1) :* varEsts(0 until m))
    }

    // Reverse the result so as to have the same convention as in the book
    SecondOrderSignature(reverse(thetaEsts(q - 1)), varEsts(q))

  }

}
