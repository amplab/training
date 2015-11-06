package main.scala.sandBox

/**
 * Created by cusgadmin on 6/9/15.
 */

import breeze.linalg._
import breeze.numerics.abs
import breeze.stats.distributions.Gaussian
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import main.scala.overlapping.containers._
import main.scala.overlapping.timeSeries._

object FrequentistAR1 {

  implicit def signedDistMillis = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

  implicit def signedDistLong = (t1: Long, t2: Long) => (t2 - t1).toDouble

  def main(args: Array[String]): Unit = {

    val d = 3
    val N = 100000L
    val paddingMillis = 100L
    val deltaTMillis = 1L
    val nPartitions = 8

    implicit val config = TSConfig(deltaTMillis, d, N, paddingMillis.toDouble)

    val conf = new SparkConf().setAppName("Counter").setMaster("local[*]")
    implicit val sc = new SparkContext(conf)

    val ARCoeffs = Array(
      DenseVector(0.45, 0.10),
      DenseVector(0.26, - 0.05),
      DenseVector(0.21, 0.07)
    )

    val p = ARCoeffs(0).length


    val noiseMagnitudes = DenseVector(0.5, 1.0, 1.0)

    val rawTS = Surrogate.generateAR(
      ARCoeffs,
      d,
      N.toInt,
      deltaTMillis,
      Gaussian(0.0, 1.0),
      noiseMagnitudes,
      sc)

    val (overlappingRDD: RDD[(Int, SingleAxisBlock[TSInstant, DenseVector[Double]])], _) =
      SingleAxisBlockRDD((paddingMillis, paddingMillis), nPartitions, rawTS)

    /*
    ################################

    Monovariate analysis

    ################################
     */

    val ARestimates = ARModel(overlappingRDD, p)

    ARestimates.foreach(println)

    AutoCovariances(overlappingRDD, p + 1).foreach(println)

  }
}