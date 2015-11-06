package main.scala.overlapping.timeSeries

import org.apache.spark.rdd.RDD
import main.scala.overlapping.IntervalSize
import main.scala.overlapping.containers.SingleAxisBlock

import scala.reflect.ClassTag


/**
 * Created by Francois Belletti on 7/10/15.
 */
abstract class SecondOrderEssStatMemory[IndexT <: Ordered[IndexT], ValueT, ResultT: ClassTag, StateT: ClassTag]
  extends Serializable{

  def kernelWidth: IntervalSize

  def modelOrder: ModelSize

  def modelWidth = modelOrder.lookAhead + modelOrder.lookBack + 1

  def zero: ResultT

  def init: StateT

  def kernel(slice: Array[(IndexT, ValueT)], state: StateT): (ResultT, StateT)

  def reducer(r1: ResultT, r2: ResultT): ResultT

  def timeSeriesStats(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, ValueT])]): ResultT = {

    timeSeries
      .mapValues(_.slidingFoldWithMemory(Array(kernelWidth))(kernel, zero, reducer, init))
      .map(_._2)
      .fold(zero)(reducer)

  }


}