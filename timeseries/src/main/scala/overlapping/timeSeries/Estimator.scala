package main.scala.overlapping.timeSeries

import breeze.linalg.DenseVector
import org.apache.spark.rdd.RDD
import main.scala.overlapping.containers.SingleAxisBlock

/**
 * Created by Francois Belletti on 9/24/15.
 */
trait Estimator[IndexT <: Ordered[IndexT], ValueT, EstimateT]
  extends Serializable{

  def estimate(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]): EstimateT

}
