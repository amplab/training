package main.scala.overlapping.timeSeries.secondOrder.multivariate.frequentistEstimators.procedures

import breeze.linalg._

/**
 * Created by Francois Belletti on 7/14/15.
 */

object ToeplitzMulti extends Serializable{

  /*
  This procedures inverts a non-symmetric Toeplitz matrix.
  R is a (n * n) Toeplitz matrix only characterized by its 2*n - 1 distinct elements.
  The R matrix is described from its upper left corner to its lower right corner.

  TODO: check size of toepM and y are compatible.
  TODO: check that diagonal element of toepM is non zero.
   */
  def apply(p: Int, d: Int, R: Array[DenseMatrix[Double]], y: Array[DenseMatrix[Double]]): Array[DenseMatrix[Double]] ={

    if(p == 1){
      return Array(R(0) \ y(0))
    }

    var prevX = Array.fill(1){DenseMatrix.zeros[Double](d, d)}
    var prevH = Array.fill(1){DenseMatrix.zeros[Double](d, d)}
    var prevG = Array.fill(1){DenseMatrix.zeros[Double](d, d)}

    val R_0 = R(p - 1)
    var R_plus = if(p > 1) R.slice(p, 2 * p) else Array(R(p))
    val R_minus = if (p > 1) R.slice(0, p - 1).reverse else Array(R(0))
    /*
    Equation system 2.8.26
     */
    prevX(0) = R_0 \ y(0)       // R(n - 1) corresponds to R(0) in the book
    prevG(0) = R_0 \ R_minus(0)
    prevH(0) = R_0 \ R_plus(0)

    var nextH = Array.fill(1){DenseMatrix.zeros[Double](d, d)}
    var nextG = Array.fill(1){DenseMatrix.zeros[Double](d, d)}
    var nextX = Array.fill(1){DenseMatrix.zeros[Double](d, d)}

    var temp_G_den = DenseMatrix.zeros[Double](d, d)
    var temp_H_den = DenseMatrix.zeros[Double](d, d)
    var temp_G_num = DenseMatrix.zeros[Double](d, d)
    var temp_H_num = DenseMatrix.zeros[Double](d, d)

    var H_den_inv = DenseMatrix.zeros[Double](d, d)
    var G_den_inv = DenseMatrix.zeros[Double](d, d)

    var G_new = DenseMatrix.zeros[Double](d, d)
    var H_new = DenseMatrix.zeros[Double](d, d)
    var X_new = DenseMatrix.zeros[Double](d, d)

    for(m <- 0 until p - 1){

      temp_H_den := - R_0
      for(i <- 0 until m + 1){
        temp_H_den += R_plus(i) * prevG(i)
      }

      H_den_inv = inv(temp_H_den)

      if(m < p - 2) {

        temp_G_den = - R_0
        for(i <- 0 until m + 1){
          temp_G_den += R_minus(i) * prevH(i)
        }

        temp_H_num = - R_plus(m + 1)
        for(i <- 0 until m + 1){
          temp_H_num += R_plus(i) * prevH(m - i)
        }

        temp_G_num = - R_minus(m + 1)
        for(i <- 0 until m + 1){
          temp_G_num += R_minus(i) * prevG(m - i)
        }

        G_den_inv = inv(temp_G_den)

        G_new = G_den_inv * temp_G_num
        H_new = H_den_inv * temp_H_num

        nextG = Array.fill(m + 2){DenseMatrix.zeros[Double](d, d)}
        nextH = Array.fill(m + 2){DenseMatrix.zeros[Double](d, d)}
        for(i <- 0 until m + 1){
          nextG(i) = prevG(i) - prevH(m - i) * G_new
          nextH(i) = prevH(i) - prevG(m - i) * H_new
        }

        nextG(m + 1) = G_new
        nextH(m + 1) = H_new

      }

      nextX = Array.fill(m + 2){DenseMatrix.zeros[Double](d, d)}

      X_new = - y(m + 1)
      for(i <- 0 until m + 1){
        X_new += R_plus(m - i) * prevX(i)
      }
      X_new = H_den_inv * X_new

      for(i <- 0 until m + 1){
        nextX(i) = prevX(i) - prevG(m - i) * X_new
      }

      nextX(m + 1) = X_new

      //Swap and allocate
      prevX = nextX
      prevH = nextH
      prevG = nextG

    }

    prevX
  }

}
