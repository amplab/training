package main.scala.overlapping.timeSeries.secondOrder.univariate.Procedures

import breeze.linalg._
import main.scala.overlapping.timeSeries._

/**
 * Created by Francois Belletti on 7/14/15.
 */

/*
  This calibrate one univariate AR model per columns.
  Returns an array of calibrated parameters (Coeffs, variance of noise).

  Check out Brockwell, Davis, Time Series: Theory and Methods, 1987 (p 234)
  TODO: shield procedure against the following edge cases, autoCov.size < 1, autoCov(0) = 0.0
   */
object DurbinLevinson extends Serializable{

  def apply(h: Int, autoCov: DenseVector[Double]): SecondOrderSignature ={

    var prevPhiEst          = DenseVector.zeros[Double](1)
    prevPhiEst(0)           = autoCov(1) / autoCov(0)
    var prevVarEst: Double  = autoCov(0) * (1.0 - prevPhiEst(0) * prevPhiEst(0))

    var newVarEst: Double   = 0.0

    for(m <- 2 to h){
      val newPhiEst               = DenseVector.zeros[Double](m)
      val temp                    = reverse(autoCov(1 until m))
      newPhiEst(m - 1)            = (autoCov(m) - sum(prevPhiEst :* temp)) / prevVarEst
      newPhiEst(0 to (m - 2))     := prevPhiEst - (reverse(prevPhiEst) :* newPhiEst(m - 1))

      newVarEst                   = prevVarEst * (1.0 - newPhiEst(m - 1) * newPhiEst(m - 1))

      prevPhiEst = newPhiEst
      prevVarEst = newVarEst
    }

    SecondOrderSignature(prevPhiEst, prevVarEst)
  }


}
