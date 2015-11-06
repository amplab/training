package main.scala.overlapping.timeSeries

import breeze.linalg
import breeze.linalg.eig.Eig
import breeze.linalg.{DenseVector, max, svd, DenseMatrix}
import breeze.numerics.{sqrt, abs}

/**
 * Created by Francois Belletti on 10/22/15.
 */
object Stability {

  def apply(coeffs: Array[DenseMatrix[Double]]): Double = {

    val d = coeffs(0).rows
    val h = coeffs.length

    val bigMatrix = DenseMatrix.zeros[Double](d * h, d * h)

    for(i <- 0 until h - 1){
      bigMatrix((i + 1) * d until (i + 2) * d, i * d until (i + 1) *d) := DenseMatrix.eye[Double](d)
    }

    for(i <- 0 until h){
      bigMatrix(0 until d, i * d until (i + 1) * d) :=  coeffs(i)
    }

    max(abs(linalg.eig(bigMatrix).eigenvalues))

  }

  def makeStable(coeffs: Array[DenseMatrix[Double]]): Unit = {
    var maxEigen = Stability.apply(coeffs)
    while(maxEigen > 0.95){
      coeffs.foreach(_ :*= 0.95 / maxEigen)
      maxEigen = Stability.apply(coeffs)
    }
  }

}
