package main.scala.overlapping.timeSeries

import org.apache.spark.rdd.RDD
import main.scala.overlapping.containers.SingleAxisBlock
import main.scala.overlapping.{IntervalSize, _}

import scala.reflect.ClassTag


/**
 * Created by Francois Belletti on 7/10/15.
 */
abstract class SecondOrderEssStat[IndexT <: Ordered[IndexT], ValueT, ResultT: ClassTag]
  extends Serializable{

  def kernelWidth: IntervalSize

  def modelOrder: ModelSize

  def modelWidth = modelOrder.lookAhead + modelOrder.lookBack + 1

  def zero: ResultT

  def kernel(slice: Array[(IndexT, ValueT)]): ResultT

  def reducer(r1: ResultT, r2: ResultT): ResultT

  def timeSeriesStats(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, ValueT])]): ResultT = {

    timeSeries
      .mapValues(_.slidingFold(Array(kernelWidth))(kernel, zero, reducer))
      .map(_._2)
      .fold(zero)(reducer)

  }


}