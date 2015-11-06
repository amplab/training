package test.scala

import breeze.linalg._
import breeze.stats.distributions.Gaussian
import main.scala.overlapping.containers.{SingleAxisBlock, SingleAxisBlockRDD}
import main.scala.overlapping.timeSeries._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}


class TestVARMAModel extends FlatSpec with Matchers{

  "The VARMA model " should " retrieve VARMA parameters." in {

    implicit def signedDistMillis = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

    implicit def signedDistLong = (t1: Long, t2: Long) => (t2 - t1).toDouble

    val d             = 3
    val N             = 1000000L
    val paddingMillis = 100L
    val deltaTMillis  = 1L
    val nPartitions   = 8

    implicit val config = TSConfig(deltaTMillis, d, N, paddingMillis.toDouble)

    val conf = new SparkConf().setAppName("Counter").setMaster("local[*]")
    implicit val sc = new SparkContext(conf)

    val p = 3
    val q = 2
    val ARcoeffs = (0 until p).toArray.map(x => DenseMatrix.rand[Double](d, d) * 0.05 + (DenseMatrix.eye[Double](d) * 0.20 / p.toDouble * (p - x).toDouble))
    val MAcoeffs = (0 until q).toArray.map(x => DenseMatrix.rand[Double](d, d) * 0.05 + (DenseMatrix.eye[Double](d) * 0.20 / q.toDouble * (q - x).toDouble))
    val noiseMagnitudes = DenseVector.ones[Double](d) + (DenseVector.rand[Double](d) * 0.2)

    val rawTS = Surrogate.generateVARMA(
      ARcoeffs,
      MAcoeffs,
      d,
      N.toInt,
      deltaTMillis,
      Gaussian(0.0, 1.0),
      noiseMagnitudes,
      sc)

    val (overlappingRDD: RDD[(Int, SingleAxisBlock[TSInstant, DenseVector[Double]])], _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), nPartitions, rawTS)

    val (estimARMACoeffs, noiseCov) = VARMAModel(overlappingRDD, p, q)

    val estimNoiseMagnitudes = diag(noiseCov)

    for(i <- 0 until p) {
      for(j <- 0 until d) {
        for(k <- 0 until d){
          estimARMACoeffs(i)(j, k) should be (ARcoeffs(i)(j, k) +- 0.25)
        }
      }
    }

    for(i <- 0 until q) {
      for(j <- 0 until d) {
        for(k <- 0 until d){
          estimARMACoeffs(p + i)(j, k) should be (MAcoeffs(i)(j, k) +- 0.25)
        }
      }
    }

    for(i <- 0 until d){
      estimNoiseMagnitudes(i) should be (noiseMagnitudes(i) +- 0.40)
    }

  }

}