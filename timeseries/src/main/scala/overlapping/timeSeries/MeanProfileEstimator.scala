package main.scala.overlapping.timeSeries

import breeze.linalg.DenseVector
import org.apache.spark.rdd.RDD
import main.scala.overlapping.containers.SingleAxisBlock

import scala.collection.mutable

/**
 * Created by Francois Belletti on 9/23/15.
 */


object MeanProfileEstimator{

  def apply[IndexT <: Ordered[IndexT]](
      timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])],
      hashFct: IndexT => Int)
     (implicit config: TSConfig): mutable.HashMap[Int, DenseVector[Double]] = {

    val meanProfileEstimator = new MeanProfileEstimator[IndexT](hashFct)

    meanProfileEstimator.estimate(timeSeries)

  }

  def removeSeason[IndexT <: Ordered[IndexT]](
      rawData: RDD[(IndexT, DenseVector[Double])],
      hashFct: IndexT => Int,
      seasonProfile: mutable.HashMap[Int, DenseVector[Double]]): RDD[(IndexT, DenseVector[Double])] = {

    val sc = rawData.context
    val seasonalProfile = sc.broadcast(seasonProfile)

    rawData.map({ case (k, v) => (k, v - seasonalProfile.value(hashFct(k))) })
  }

}


class MeanProfileEstimator[IndexT <: Ordered[IndexT]](
    hashFct: IndexT => Int)
    (implicit config: TSConfig)
  extends FirstOrderEssStat[IndexT, DenseVector[Double], mutable.HashMap[Int, (DenseVector[Double], Long)]]
  with Estimator[IndexT, DenseVector[Double], mutable.HashMap[Int, DenseVector[Double]]]{

  override def zero = new mutable.HashMap[Int, (DenseVector[Double], Long)]()

  override def kernel(datum: (IndexT,  DenseVector[Double])): mutable.HashMap[Int, (DenseVector[Double], Long)] = {
    mutable.HashMap(hashFct(datum._1) -> (datum._2, 1L))
  }

  def merge(x: (DenseVector[Double], Long), y: (DenseVector[Double], Long)): (DenseVector[Double], Long) = {
    (x._1 + y._1, x._2 + y._2)
  }

  override def reducer(map1: mutable.HashMap[Int, (DenseVector[Double], Long)],
                       map2: mutable.HashMap[Int, (DenseVector[Double], Long)]):
  mutable.HashMap[Int, (DenseVector[Double], Long)] = {

    for(k <- map2.keySet){
        map1(k) = merge(map2(k), map1.getOrElse(k, (DenseVector.zeros[Double](config.d), 0L)))
    }

    map1

  }

  def normalize(map: mutable.HashMap[Int, (DenseVector[Double], Long)]): mutable.HashMap[Int, DenseVector[Double]] = {
    map.map({case (k, (v1, v2)) => (k, v1 / v2.toDouble)})
  }

  override def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]): mutable.HashMap[Int, DenseVector[Double]] = {
    normalize(timeSeriesStats(timeSeries))
  }

}
