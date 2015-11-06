package main.scala.overlapping

import breeze.linalg.DenseVector
import org.apache.spark.rdd.RDD
import main.scala.overlapping.containers.SingleAxisBlock
import main.scala.overlapping.timeSeries.TSInstant

import scala.reflect.ClassTag

/**
 * Created by Francois Belletti on 10/12/15.
 */

object Utils {

  def resample[T: ClassTag, V: ClassTag](
      in: RDD[(TSInstant, V)],
      hasher: TSInstant => TSInstant,
      preMap: ((TSInstant, V)) => (TSInstant, T),
      reducer: (T, T) => T,
      postReduce: ((TSInstant, T)) => (TSInstant, V)): RDD[(TSInstant, V)] = {

    in
      .map({case (t, v) => preMap(hasher(t), v)})
      .reduceByKey(reducer)
      .map(postReduce)

  }

  def resample[V: ClassTag](
      in: RDD[(TSInstant, V)],
      hasher: TSInstant => TSInstant,
      reducer: (V, V) => V): RDD[(TSInstant, V)] = {

    in
      .map({case (t, v) => (hasher(t), v)})
      .reduceByKey(reducer)

  }

  def sumResample(
      in: RDD[(TSInstant, DenseVector[Double])],
      hasher: TSInstant => TSInstant): RDD[(TSInstant, DenseVector[Double])] = {

    resample[DenseVector[Double]](
      in,
      hasher,
      _ + _
    )

  }

  def meanResample(
      in: RDD[(TSInstant, DenseVector[Double])],
      hasher: TSInstant => TSInstant): RDD[(TSInstant, DenseVector[Double])] = {

    resample[(DenseVector[Double], Long), DenseVector[Double]](
      in,
      hasher,
      {case (t: TSInstant, v: DenseVector[Double]) => (t, (v, 1L))},
      {case ((v1: DenseVector[Double], c1: Long), (v2: DenseVector[Double], c2: Long)) => (v1 + v2, c1 + c2)},
      {case (t: TSInstant, (v: DenseVector[Double], c: Long)) => (t, v / c.toDouble)}
    )

  }

}


class Utils {

  type RawTS = RDD[(TSInstant, DenseVector[Double])]

  type OverlappingTS = RDD[(Int, SingleAxisBlock[TSInstant, DenseVector[Double]])]

  implicit def signedDistMillis = (t1: TSInstant, t2: TSInstant) => (t2.timestamp.getMillis - t1.timestamp.getMillis).toDouble

  implicit def signedDistLong = (t1: Long, t2: Long) => (t2 - t1).toDouble

}
