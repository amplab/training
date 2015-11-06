package main.scala.sandBox

/**
 * Created by cusgadmin on 6/9/15.
 */

import breeze.linalg._
import breeze.stats.distributions.Gaussian
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

import main.scala.overlapping._
import main.scala.overlapping.timeSeries._

object TryResampling {

  implicit def signedDistMillis = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

  implicit def signedDistLong = (t1: Long, t2: Long) => (t2 - t1).toDouble

  def main(args: Array[String]): Unit = {

    val d = 30
    val N = 100000L
    val paddingMillis = 100L
    val deltaTMillis = 1L
    val resolutionMillis = 10L
    val nPartitions = 8

    implicit val config = TSConfig(deltaTMillis, d, N, paddingMillis.toDouble)

    val conf = new SparkConf().setAppName("Counter").setMaster("local[*]")
    implicit val sc = new SparkContext(conf)

    val A = DenseMatrix.rand[Double](d, d) + (DenseMatrix.eye[Double](d) * 0.1)
    val svd.SVD(_, sA, _) = svd(A)
    A :*= 1.0 / (max(sA) * 1.1)
    val ARcoeffs = Array(A)
    val noiseMagnitudes = DenseVector.ones[Double](d) + (DenseVector.rand[Double](d) * 0.2)

    /*
    val rawTS = IndividualRecords.generateVAR(
      ARcoeffs,
      d,
      N.toInt,
      deltaTMillis,
      Gaussian(0.0, 1.0),
      noiseMagnitudes,
      sc)
    */

    val rawTS = Surrogate.generateOnes(d, N.toInt, deltaTMillis, sc)

    def hasher(t: TSInstant): TSInstant = {
      TSInstant(new DateTime((t.timestamp.getMillis / resolutionMillis) * resolutionMillis))
    }

    val temp0 = rawTS.collect()
    val temp1 = Utils.meanResample(rawTS, hasher).sortBy(_._1).collect()
    val temp2 = Utils.sumResample(rawTS, hasher).sortBy(_._1).collect()

    println("Done")

  }
}