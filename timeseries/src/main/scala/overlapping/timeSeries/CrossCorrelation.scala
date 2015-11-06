package main.scala.overlapping.timeSeries

import breeze.linalg._
import breeze.numerics.sqrt
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import main.scala.overlapping._
import main.scala.overlapping.containers.SingleAxisBlock

import scala.reflect.ClassTag


/**
 * Created by Francois Belletti on 7/10/15.
 */

object CrossCorrelation{

  def apply[IndexT <: Ordered[IndexT] : ClassTag](
      timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])],
      maxLag: Int,
      mean: Option[DenseVector[Double]] = None)
      (implicit config: TSConfig): (Array[DenseMatrix[Double]], DenseMatrix[Double]) ={

    implicit val sc = timeSeries.context
    val estimator = new CrossCorrelation[IndexT](maxLag, mean)
    estimator.estimate(timeSeries)

  }

}

/**
Here we expect the number of dimensions to be the same for all records.

The autocovoriance is ordered as follows

-modelOrder ... 0 ... modelOrder
 */

class CrossCorrelation[IndexT <: Ordered[IndexT] : ClassTag](
    maxLag: Int,
    mean: Option[DenseVector[Double]] = None)
    (implicit config: TSConfig, sc: SparkContext)
  extends SecondOrderEssStat[IndexT, DenseVector[Double], (Array[DenseMatrix[Double]], DenseVector[Double])]
  with Estimator[IndexT, DenseVector[Double], (Array[DenseMatrix[Double]], DenseMatrix[Double])]{

  val d = config.d
  val deltaT = config.deltaT

  if(deltaT * maxLag > config.padding){
    throw new IndexOutOfBoundsException("Not enough padding to support model estimation.")
  }

  val bcMean = sc.broadcast(if (mean.isDefined) mean.get else DenseVector.zeros[Double](d))

  override def kernelWidth = IntervalSize(deltaT * maxLag, deltaT * maxLag)

  override def modelOrder = ModelSize(maxLag, maxLag)

  override def zero = (Array.fill(modelWidth){DenseMatrix.zeros[Double](d, d)}, DenseVector.zeros[Double](d))

  override def kernel(slice: Array[(IndexT, DenseVector[Double])]): (Array[DenseMatrix[Double]], DenseVector[Double]) = {

    val result = Array.fill(modelOrder.lookBack + modelOrder.lookAhead + 1)(DenseMatrix.zeros[Double](d, d))
    val normalizer = DenseVector.zeros[Double](d)

    // The slice is not full size, it shall not be considered in order to avoid redundant computations
    if(slice.length != modelWidth){
      return (result, normalizer)
    }

    val meanValue = bcMean.value
    val centerTarget  = slice(modelOrder.lookBack)._2 - meanValue

    normalizer += centerTarget :* centerTarget

    var i, c1, c2 = 0
    while(i <= modelOrder.lookBack){

      //result(i) :+= centerTarget * (slice(i)._2 - meanValue).t

      val currentTarget = slice(i)._2 - meanValue
      c1 = 0
      while(c1 < d){
        c2 = c1
        while(c2 < d){
          result(i)(c1, c2) += centerTarget(c1) * currentTarget(c2)
          c2 += 1
        }
        c1 += 1
      }

      i += 1
    }

    (result, normalizer)
  }

  override def reducer(
      x: (Array[DenseMatrix[Double]], DenseVector[Double]),
      y: (Array[DenseMatrix[Double]], DenseVector[Double])): (Array[DenseMatrix[Double]], DenseVector[Double]) ={
    (x._1.zip(y._1).map({case (u, v) => u :+ v}), x._2 + y._2)
  }

  def normalize(r: (Array[DenseMatrix[Double]], DenseVector[Double])): Array[DenseMatrix[Double]] = {

    println(r._1)
    println(r._2)

    var i, c1, c2 = 0

    while(i < r._1.length) {
      c1 = 0
      while (c1 < d) {
        c2 = c1
        while (c2 < d) {
          r._1(i)(c1, c2) /= sqrt(r._2(c1) * r._2(c2))
          c2 += 1
        }
        c1 += 1
      }
      i += 1
    }

    i = 0
    c1 = 0
    c2 = 0

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

    r._1

  }

  override def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]):
    (Array[DenseMatrix[Double]], DenseMatrix[Double])={

    val covarianceMatrices = normalize(
      timeSeriesStats(timeSeries)
    )

    (covarianceMatrices, covarianceMatrices(modelOrder.lookBack))

  }


}