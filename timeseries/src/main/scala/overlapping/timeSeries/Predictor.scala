package main.scala.overlapping.timeSeries

import breeze.linalg.DenseVector
import org.apache.spark.rdd.RDD
import main.scala.overlapping._
import main.scala.overlapping.containers.SingleAxisBlock

/**
 * Created by Francois Belletti on 9/24/15.
 */
trait Predictor[IndexT <: Ordered[IndexT]]
  extends Serializable{

  def size: Array[IntervalSize]

  def predictKernel(data: Array[(IndexT, DenseVector[Double])]): DenseVector[Double]

  def residualKernel(data: Array[(IndexT, DenseVector[Double])]): DenseVector[Double] = {
    data(data.length - 1)._2 - predictKernel(data)
  }

  def estimateResiduals(
      timeSeries: RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])]): RDD[(Int, SingleAxisBlock[IndexT, DenseVector[Double]])] = {

    timeSeries
      .mapValues(_.sliding(size)(residualKernel))

  }


}
