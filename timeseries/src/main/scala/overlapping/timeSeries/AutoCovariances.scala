package main.scala.overlapping.timeSeries

import breeze.linalg.{DenseVector, reverse}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import main.scala.overlapping._
import main.scala.overlapping.containers.SingleAxisBlock

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 9/25/15.
 */
object AutoCovariances {

  def apply[IndexT <: Ordered[IndexT] : ClassTag](
      timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])],
      maxLag: Int,
      mean: Option[DenseVector[Double]] = None)
      (implicit config: TSConfig): Array[SecondOrderSignature] = {

    implicit val sc = timeSeries.context
    val estimator  = new AutoCovariances[IndexT](maxLag, mean)
    estimator.estimate(timeSeries)

  }

}


class AutoCovariances[IndexT <: Ordered[IndexT] : ClassTag](
    maxLag: Int,
    mean: Option[DenseVector[Double]] = None)
    (implicit config: TSConfig, sc: SparkContext)
  extends SecondOrderEssStat[IndexT, DenseVector[Double], (Array[SecondOrderSignature], Long)]
  with Estimator[IndexT, DenseVector[Double], Array[SecondOrderSignature]]
{

  val deltaT = config.deltaT

  if(deltaT * maxLag > config.padding){
    throw new IndexOutOfBoundsException("Not enough padding to support model estimation.")
  }

  val d = config.d
  val bcMean = sc.broadcast(mean.getOrElse(DenseVector.zeros[Double](d)))

  def kernelWidth = IntervalSize(deltaT * maxLag, 0)

  def modelOrder = ModelSize(maxLag, 0)

  def zero = (Array.fill(d){SecondOrderSignature(DenseVector.zeros[Double](modelWidth), 0.0)}, 0L)

  override def kernel(slice: Array[(IndexT, DenseVector[Double])]): (Array[SecondOrderSignature], Long) = {

    val tempCovs = Array.fill(d){DenseVector.zeros[Double](modelWidth)}
    val tempVars = Array.fill(d){0.0}

    val meanValue = bcMean.value

    /*
    The slice is not full size, it shall not be considered in order to avoid redundant computations
     */
    if(slice.length != modelWidth){
      return (Array.fill(d){SecondOrderSignature(DenseVector.zeros[Double](modelWidth), 0.0)}, 0L)
    }

    for(c <- 0 until d){

      val centerTarget  = slice(modelOrder.lookBack)._2(c) - meanValue(c)
      tempVars(c) += centerTarget * centerTarget

      for(i <- 0 to modelOrder.lookBack){
        tempCovs(c)(i) += centerTarget * (slice(i)._2(c) - meanValue(c))
      }

    }

    (tempCovs.zip(tempVars).map({case (x, y) => SecondOrderSignature(x, y)}), 1L)

  }

  override def reducer(x: (Array[SecondOrderSignature], Long), y: (Array[SecondOrderSignature], Long)):
    (Array[SecondOrderSignature], Long) = {

    (x._1.zip(y._1).map({case (SecondOrderSignature(cov1, v1), SecondOrderSignature(cov2, v2)) => SecondOrderSignature(cov1 + cov2, v1 + v2)}), x._2 + y._2)

  }


  override def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]):
    Array[SecondOrderSignature] = {

    val (covSigns: Array[SecondOrderSignature], nSamples: Long) = timeSeriesStats(timeSeries)

    covSigns.map(x => SecondOrderSignature(reverse(x.covariation) / nSamples.toDouble, x.variation / nSamples.toDouble))

  }

}
