package main.scala.overlapping.timeSeries

import breeze.linalg._
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import main.scala.overlapping._
import main.scala.overlapping.containers.SingleAxisBlock

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 7/10/15.
 */

object CrossCovariance{

  def apply[IndexT <: Ordered[IndexT] : ClassTag](
      timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])],
      maxLag: Int,
      mean: Option[DenseVector[Double]] = None)
      (implicit config: TSConfig): (Array[DenseMatrix[Double]], DenseMatrix[Double]) ={

    implicit val sc = timeSeries.context
    val estimator = new CrossCovariance[IndexT](maxLag, mean)
    estimator.estimate(timeSeries)

  }

}

/**
Here we expect the number of dimensions to be the same for all records.

The autocovoriance is ordered as follows

-modelOrder ... 0 ... modelOrder
 */

class CrossCovariance[IndexT <: Ordered[IndexT] : ClassTag](
    maxLag: Int,
    mean: Option[DenseVector[Double]] = None)
    (implicit config: TSConfig, sc: SparkContext)
  extends SecondOrderEssStat[IndexT, DenseVector[Double], (Array[DenseMatrix[Double]], Long)]
  with Estimator[IndexT, DenseVector[Double], (Array[DenseMatrix[Double]], DenseMatrix[Double])]{

  val d = config.d
  val deltaT = config.deltaT

  if(deltaT * maxLag > config.padding){
    throw new IndexOutOfBoundsException("Not enough padding to support model estimation.")
  }

  val bcMean = sc.broadcast(if (mean.isDefined) mean.get else DenseVector.zeros[Double](d))

  override def kernelWidth = IntervalSize(deltaT * maxLag, deltaT * maxLag)

  override def modelOrder = ModelSize(maxLag, maxLag)

  override def zero = (Array.fill(modelWidth){DenseMatrix.zeros[Double](d, d)}, 0L)

  override def kernel(slice: Array[(IndexT, DenseVector[Double])]): (Array[DenseMatrix[Double]], Long) = {

    val result = Array.fill(modelOrder.lookBack + modelOrder.lookAhead + 1)(DenseMatrix.zeros[Double](d, d))

    // The slice is not full size, it shall not be considered in order to avoid redundant computations
    if(slice.length != modelWidth){
      return (result, 0L)
    }

    val meanValue = bcMean.value
    val centerTarget  = slice(modelOrder.lookBack)._2 - meanValue

    var i = 0
    var c1, c2 = 0
    while(i <= modelOrder.lookBack){

      //result(i) :+= centerTarget * (slice(i)._2 - meanValue).t

      val currentTarget = slice(i)._2 - meanValue
      c1 = 0
      while(c1 < d){
        c2 = c1
        while(c2 < d) {
          result(i)(c1, c2) += centerTarget(c1) * currentTarget(c2)
          c2 += 1
        }
        c1 += 1
      }

      i += 1
    }

    (result, 1L)

  }

  override def reducer(
      x: (Array[DenseMatrix[Double]], Long),
      y: (Array[DenseMatrix[Double]], Long)): (Array[DenseMatrix[Double]], Long) ={
    (x._1.zip(y._1).map({case (u, v) => u :+ v}), x._2 + y._2)
  }

  def normalize(r: (Array[DenseMatrix[Double]], Long)): Array[DenseMatrix[Double]] = {

    var i, c1, c2 = 0

    while(i <= modelOrder.lookBack){
      c1 = 0
      while(c1 < d){
        c2 = 0
        while(c2 < c1) {
          r._1(i)(c1, c2) = r._1(i)(c2, c1)
          c2 += 1
        }
        c1 += 1
      }
      i += 1
    }

    i = 1
    while(i <= modelOrder.lookAhead){
      r._1(modelOrder.lookBack + i) = r._1(modelOrder.lookBack - i).t
      i += 1
    }

    r._1.map(_ / r._2.toDouble)
  }

  override def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]):
    (Array[DenseMatrix[Double]], DenseMatrix[Double])={

    val covarianceMatrices = normalize(
      timeSeriesStats(timeSeries)
    )

    (covarianceMatrices, covarianceMatrices(modelOrder.lookBack))

  }


}