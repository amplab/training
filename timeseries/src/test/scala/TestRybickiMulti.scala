package test.scala

/**
 * Created by Francois Belletti on 8/17/15.
 */

import breeze.linalg.DenseMatrix
import main.scala.overlapping.timeSeries.secondOrder.multivariate.frequentistEstimators.procedures.ToeplitzMulti
import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by Francois Belletti on 8/4/15.
 */
class TestRybickiMulti extends FlatSpec with Matchers{

  "The Rybicki procedure " should " properly solve a block Toeplitz system" in {

    val p           = 3
    val d           = 1

    val blockArray  = Array.fill(2 * p - 1){DenseMatrix.rand[Double](d, d)}
    val y           = Array.fill(p){DenseMatrix.rand[Double](d, d)}

    val blockToeplitzMatrix = DenseMatrix.zeros[Double](p * d, p * d)

    for(i <- 0 until p){
      for(j <- 0 until p){
        blockToeplitzMatrix((i * d) until ((i + 1) * d), (j * d) until ((j + 1) * d)) := blockArray(i + p - j - 1)
      }
    }

    val x = ToeplitzMulti(p, d, blockArray, y)

    val xMatrix = DenseMatrix.zeros[Double](d * p, d)
    val yMatrix = DenseMatrix.zeros[Double](d * p, d)

    for(i <- 0 until p){
      xMatrix((i * d) until ((i + 1) * d), ::) := x(i)
      yMatrix((i * d) until ((i + 1) * d), ::) := y(i)
    }

    val yCheck: DenseMatrix[Double] = blockToeplitzMatrix * xMatrix

    for(i <- 0 until d * p){
      for(j <- 0 until d){
        yMatrix(i, j) should be (yCheck(i, j) +- 0.0000001)
      }
    }

  }

}