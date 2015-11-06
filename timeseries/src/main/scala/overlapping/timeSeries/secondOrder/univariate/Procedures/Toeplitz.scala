package main.scala.overlapping.timeSeries.secondOrder.univariate.Procedures

import breeze.linalg._

/**
 * Created by Francois Belletti on 7/14/15.
 */

object Toeplitz extends Serializable{

  /*
  This procedures inverts a non-symmetric Toeplitz matrix.
  R is a (p * p) Toeplitz matrix only characterized by its 2*p - 1 distinct elements.
  The R matrix is described from its upper left corner to its lower right corner.

  TODO: check size of toepM and y are compatible.
  TODO: check that diagonal element of toepM is non zero.
   */
  def apply(p: Int, R: DenseVector[Double], y: DenseVector[Double]): DenseVector[Double] ={
    var prevX = DenseVector.zeros[Double](1)
    var prevH = DenseVector.zeros[Double](1)
    var prevG = DenseVector.zeros[Double](1)

    if(p == 1){
      return DenseVector(y(0) / R(0))
    }

    /*
    Equation system 2.8.26
     */
    prevX(0) = y(0) / R(p - 1)       // R(n - 1) corresponds to R(0) in the book
    prevG(0) = R(p - 2) / R(p - 1)
    prevH(0) = R(p) / R(p - 1)

    var nextH = DenseVector.zeros[Double](2)
    var nextG = DenseVector.zeros[Double](2)
    var nextX = DenseVector.zeros[Double](2)
    for(m <- 0 until p - 1){

      if(m < p - 2) {
        // Equation 2.8.23
        nextH(m + 1) = (sum(R(m + p to p by -1) :* prevH) - R(m + p + 1)) / (sum(R(m + p to p by -1) :* reverse(prevG)) - R(p - 1))

        // Equation 2.8.24
        nextG(m + 1) = (sum(R(p - m - 2 to p - 2) :* prevG) - R(p - m - 3)) / (sum(R(p - m - 2 to p - 2) :* reverse(prevH)) - R(p - 1))

        // Equation system 2.8.25
        nextH(0 to m) := prevH - (reverse(prevG) * nextH(m + 1))
        nextG(0 to m) := prevG - (reverse(prevH) * nextG(m + 1))
      }

      // Equation 2.8.19
      // TODO: computation of numerator is redundent with that of nextH
      nextX(m + 1) = (sum(R(m + p to p by -1) :* prevX) - y(m + 1)) / (sum(R(m + p to p by - 1) :* reverse(prevG)) - R(p - 1))

      // Equation 2.8.16
      nextX(0 to m) := prevX - (reverse(prevG) * nextX(m + 1))

      //Swap and allocate
      prevX = nextX
      prevH = nextH
      prevG = nextG

      nextX = DenseVector.zeros[Double](m + 3)
      nextG = DenseVector.zeros[Double](m + 3)
      nextH = DenseVector.zeros[Double](m + 3)

    }

    prevX
  }

}
