package test.scala

import breeze.linalg._
import breeze.numerics._
import breeze.stats.distributions.Gaussian
import main.scala.overlapping.containers.{SingleAxisBlock, SingleAxisBlockRDD}
import main.scala.overlapping.timeSeries._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}


class TestAutocov extends FlatSpec with Matchers{

  "The autocovariance " should " solve the Yule-Walker system of equations." in {

    implicit def signedDistMillis = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

    implicit def signedDistLong = (t1: Long, t2: Long) => (t2 - t1).toDouble

    val d             = 3
    val N             = 100000L
    val paddingMillis = 100L
    val deltaTMillis  = 1L
    val nPartitions   = 8

    implicit val config = TSConfig(deltaTMillis, d, N, paddingMillis.toDouble)

    val conf = new SparkConf().setAppName("Counter").setMaster("local[*]")
    implicit val sc = new SparkContext(conf)

    val phis = Array(DenseVector(0.30, -0.17, -0.08),
      DenseVector(0.20, 0.12, 0.03),
      DenseVector(-0.21, 0.07, 0.04))

    val ARcoeffs = Array(
      DenseMatrix((phis(0)(0), 0.0, 0.0), (0.0, phis(1)(0), 0.0), (0.0, 0.0, phis(2)(0))),
      DenseMatrix((phis(0)(1), 0.0, 0.0), (0.0, phis(1)(1), 0.0), (0.0, 0.0, phis(2)(1))),
      DenseMatrix((phis(0)(2), 0.0, 0.0), (0.0, phis(1)(2), 0.0), (0.0, 0.0, phis(2)(2))))

    val noiseMagnitudes = DenseVector.ones[Double](d) + (DenseVector.rand[Double](d) * 0.2)

    val rawTS = Surrogate.generateVAR(
      ARcoeffs,
      d,
      N.toInt,
      deltaTMillis,
      Gaussian(0.0, 1.0),
      noiseMagnitudes,
      sc)

    val (overlappingRDD: RDD[(Int, SingleAxisBlock[TSInstant, DenseVector[Double]])], _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), nPartitions, rawTS)

    val p = 3
    val autocovs = AutoCovariances(overlappingRDD, p)

    for(i <- 0 until d) {
      val toeplitzMatrix = DenseMatrix.zeros[Double](p, p)

      for (j <- 0 until p) {
        for (k <- 0 until p) {
          toeplitzMatrix(j, k) = autocovs(i).covariation(abs(j - k))
        }
      }

      val residual = abs(toeplitzMatrix * phis(i) - autocovs(i).covariation(1 to p))

      for (j <- 0 until p) {
        residual(j) should be(0.0 +- 0.10)
      }

    }

  }

}