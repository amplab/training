package test.scala

/**
 * Created by Francois Belletti on 8/17/15.
 */

import breeze.linalg.{DenseMatrix, DenseVector}
import main.scala.overlapping.timeSeries.secondOrder.univariate.Procedures.Toeplitz
import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by Francois Belletti on 8/4/15.
 */
class TestRybicki extends FlatSpec with Matchers{

  "The Ribicky procedure " should " properly solve a full Toeplitz system" in {

    val n       = 3
    //val RVector = DenseVector.rand[Double](2 * n - 1)
    //val y       = DenseVector.rand[Double](n)

    val RVector = DenseVector(1.20563493, 1.67256029, 1.20479096, -1.28413286, -1.8205618)
    val y       = DenseVector(0.6956028, 1.58059648, 0.6352319)

    val toeplitzMatrix = DenseMatrix.zeros[Double](n, n)

    for(i <- 0 until n){
      for(j <- 0 until n){
        toeplitzMatrix(i, j) = RVector(i + n - j - 1)
      }
    }

    //println(toeplitzMatrix)

    val x = Toeplitz(n, RVector, y)

    //println(x)

    val yCheck: DenseVector[Double] = toeplitzMatrix * x

    //println()
    //println(yCheck)
    //println(y)

    for(i <- 0 until n){
      y(i) should be (yCheck(i) +- 0.0000001)
    }

  }

}