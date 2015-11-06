package main.scala.overlapping.timeSeries

import org.apache.spark.rdd.RDD
import main.scala.overlapping._
import main.scala.overlapping.containers.SingleAxisBlock

import scala.reflect.ClassTag


/**
 * Created by Francois Belletti on 7/10/15.
 */
abstract class FirstOrderEssStat[IndexT <: Ordered[IndexT], ValueT, ResultT: ClassTag]
  extends Serializable{

  def zero: ResultT

  def kernel(datum: (IndexT, ValueT)): ResultT

  def reducer(r1: ResultT, r2: ResultT): ResultT

  def timeSeriesStats(timeSeries: RDD[(Int, SingleAxisBlock[IndexT, ValueT])]): ResultT = {

    timeSeries
      .mapValues(_.fold(zero)({case (x, y) => kernel(x, y)}, reducer))
      .map(_._2)
      .fold(zero)(reducer)

  }


}