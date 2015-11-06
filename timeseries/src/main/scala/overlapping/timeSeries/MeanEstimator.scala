package main.scala.overlapping.timeSeries

import breeze.linalg.DenseVector
import org.apache.spark.rdd.RDD
import main.scala.overlapping.containers.SingleAxisBlock

/**
 * Created by Francois Belletti on 9/23/15.
 */

object MeanEstimator{

  /**
   * Compute the mean of a Time Series RDD.
   *
   * @param timeSeries Input data.
   * @param config Configuration of input data.
   * @tparam IndexT Timestamp type.
   * @return Dimension-wise mean.
   */
  def apply[IndexT <: Ordered[IndexT]](
      timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])])
      (implicit config: TSConfig): DenseVector[Double] ={

    implicit val sc = timeSeries.context
    val estimator = new MeanEstimator[IndexT]()
    estimator.estimate(timeSeries)

  }

}

/**
 * This class is dedicated to estimating the mean of a distributed time series.
 *
 * @param config Configuration of the data.
 * @tparam IndexT Timestamp type.
 */
class MeanEstimator[IndexT <: Ordered[IndexT]](implicit config: TSConfig)
  extends FirstOrderEssStat[IndexT, DenseVector[Double], (DenseVector[Double], Long)]
  with Estimator[IndexT, DenseVector[Double], DenseVector[Double]]{

  override def zero = (DenseVector.zeros[Double](config.d), 0L)

  override def kernel(datum: (IndexT,  DenseVector[Double])):  (DenseVector[Double], Long) = {
    (datum._2, 1L)
  }

  override def reducer(r1: (DenseVector[Double], Long), r2: (DenseVector[Double], Long)): (DenseVector[Double], Long) = {
    (r1._1 + r2._1, r1._2 + r2._2)
  }

  def normalize(x: (DenseVector[Double], Long)): DenseVector[Double] = {
    x._1 / x._2.toDouble
  }

  override def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]): DenseVector[Double] = {
    normalize(timeSeriesStats(timeSeries))
  }

}
